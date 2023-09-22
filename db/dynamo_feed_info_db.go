package db

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/shenyute/realtime_message_dispatch/core"
	"github.com/shenyute/realtime_message_dispatch/protocol"
	"go.uber.org/zap"
	"time"
)

type DynamoFeedInfoDb struct {
	logger    *zap.Logger
	db        *dynamodb.Client
	tableName *string
	region    string
}

func (d *DynamoFeedInfoDb) GetFeedInfo(f *protocol.Feed) (*core.FeedInfoItem, error) {
	ctx, cancelFn := context.WithTimeout(context.TODO(), time.Second*5)
	defer cancelFn()
	feedKey := core.GetFeedKey(f)
	keyCond := expression.Key("FeedKey").Equal(expression.Value(feedKey))
	builder := expression.NewBuilder().WithKeyCondition(keyCond)
	expr, err := builder.Build()
	queryOutput, err := d.db.Query(ctx, &dynamodb.QueryInput{
		Limit:                     aws.Int32(1),
		ConsistentRead:            aws.Bool(true),
		TableName:                 d.tableName,
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ReturnConsumedCapacity:    types.ReturnConsumedCapacityIndexes,
	})

	if err != nil {
		return nil, err
	}
	LogConsumedUnits(*d.tableName, "", "get", queryOutput.ConsumedCapacity)
	if queryOutput.Count == 0 {
		return nil, nil
	} else if queryOutput.Count != 1 {
		return nil, core.DuplicateNodeInfo
	}
	var item core.FeedInfoItem
	if err := attributevalue.UnmarshalMap(queryOutput.Items[0], &item); err != nil {
		return nil, err
	}

	now := time.Now().Unix()
	// Already expire
	if item.Ttl < now {
		return nil, nil
	}

	return &item, nil
}

func (d *DynamoFeedInfoDb) UpdateFeedInfo(f *protocol.Feed, info *core.FeedInfoItem) error {
	ctx, cancelFn := context.WithTimeout(context.TODO(), time.Second*5)
	info.FeedKey = core.GetFeedKey(f)
	info.TypeId = "virtualStream"
	defer cancelFn()
	if av, err := attributevalue.MarshalMap(info); err != nil {
		return err
	} else {
		var r *dynamodb.PutItemOutput
		r, err = d.db.PutItem(ctx, &dynamodb.PutItemInput{
			TableName:              d.tableName,
			Item:                   av,
			ReturnConsumedCapacity: types.ReturnConsumedCapacityIndexes,
		})
		if err != nil {
			return err
		}
		LogConsumedUnits(*d.tableName, "", "put", r.ConsumedCapacity)
	}
	return nil
}

func (d *DynamoFeedInfoDb) DeleteFeedInfo(f *protocol.Feed) error {
	feedKey := core.GetFeedKey(f)
	input := &dynamodb.DeleteItemInput{
		Key: map[string]types.AttributeValue{
			"FeedKey": &types.AttributeValueMemberS{Value: feedKey},
			"TypeId":  &types.AttributeValueMemberS{Value: "virtualStream"},
		},
		ReturnConsumedCapacity: types.ReturnConsumedCapacityIndexes,
		TableName:              d.tableName,
	}
	ctx, cancelFn := context.WithTimeout(context.TODO(), time.Second*5)
	defer cancelFn()
	r, err := d.db.DeleteItem(ctx, input)
	if err != nil {
		return err
	}
	LogConsumedUnits(*d.tableName, "", "delete", r.ConsumedCapacity)
	return nil
}

func CreateDynamoFeedInfoDb(region, stableName string) (core.FeedInfoDb, error) {
	logger := zap.L().Named("DynamoFeedInfoDb")
	logger.Info("Create db with region", zap.String("region", region))
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
		config.WithHTTPClient(GetDynamoHttpClient()),
	)
	if err != nil {
		return nil, err
	}
	var client *dynamodb.Client

	// Create the DynamoDB service client to make the query request with.
	client = dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.Region = region
	})
	db := &DynamoFeedInfoDb{
		logger:    logger,
		db:        client,
		tableName: aws.String(stableName),
		region:    region,
	}
	return db, nil
}
