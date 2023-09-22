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
	"go.uber.org/zap"
	"time"
)

type DynamoNodeStatDb struct {
	logger    *zap.Logger
	db        *dynamodb.Client
	tableName *string
	ipV6      string
	ip        string
	privateIp string
	port      string
	nodeId    string
	region    string
	ttl       time.Duration
}

func (d *DynamoNodeStatDb) Init() {
}

func (d *DynamoNodeStatDb) GetNodeInfo(nodeId string) (*core.NodeStatInfo, error) {
	ctx, cancelFn := context.WithTimeout(context.TODO(), time.Second*5)
	defer cancelFn()
	keyCond := expression.Key("NodeId").Equal(expression.Value(nodeId))
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
	var item core.NodeStatInfo
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

func (d *DynamoNodeStatDb) ListNodeInfos() ([]*core.NodeStatInfo, error) {
	ctx, cancelFn := context.WithTimeout(context.TODO(), time.Second*5)
	defer cancelFn()
	result, err := d.db.Scan(ctx, &dynamodb.ScanInput{
		Limit:                  aws.Int32(50),
		TableName:              d.tableName,
		ReturnConsumedCapacity: types.ReturnConsumedCapacityIndexes,
	})
	if err != nil {
		return nil, err
	}
	LogConsumedUnits(*d.tableName, "", "list", result.ConsumedCapacity)
	stats := make([]*core.NodeStatInfo, 0, len(result.Items))
	for _, entry := range result.Items {
		var stat core.NodeStatInfo
		if err := attributevalue.UnmarshalMap(entry, &stat); err != nil {
			continue
		}
		now := time.Now().Unix()
		// Already expire
		if stat.Ttl < now {
			continue
		}
		stats = append(stats, &stat)
	}
	return stats, nil
}

func (d *DynamoNodeStatDb) UpdateNodeInfo(nodeId string, info *core.NodeStatInfo) error {
	return d.PutItem(nodeId, info)
}

func (d *DynamoNodeStatDb) DeleteNodeInfo(nodeId string) error {
	input := &dynamodb.DeleteItemInput{
		Key: map[string]types.AttributeValue{
			"NodeId": &types.AttributeValueMemberS{Value: nodeId},
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

func (d *DynamoNodeStatDb) PutItem(nodeId string, info *core.NodeStatInfo) error {
	ctx, cancelFn := context.WithTimeout(context.TODO(), time.Second*5)
	defer cancelFn()
	info.Ttl = time.Now().Add(d.ttl + time.Second*10).Unix()
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

func CreateDynamoNodeStatDB(region, tableName, IP, PrivateIP, IPv6, nodeId string, ttl time.Duration, addrPort string) (core.NodeStatDb, error) {
	logger := zap.L().Named("DynamoNodeStatDB")
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

	//hostname, _ := os.Hostname()
	cdb := &DynamoNodeStatDb{
		logger:    logger,
		db:        client,
		tableName: aws.String(tableName),
		ttl:       ttl,
		ipV6:      IPv6,
		ip:        IP,
		privateIp: PrivateIP,
		port:      addrPort,
		nodeId:    nodeId,
		region:    region,
	}
	return cdb, nil
}
