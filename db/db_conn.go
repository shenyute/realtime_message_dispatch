package db

import (
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"net/http"
	"time"
)

var dynamoHttpClient *awshttp.BuildableClient

var consumedUnitsCounter *prometheus.CounterVec

func init() {
	dynamoHttpClient = awshttp.NewBuildableClient().WithTimeout(time.Second * 10).WithTransportOptions(func(tr *http.Transport) {
		tr.MaxIdleConnsPerHost = 10
	})
	consumedUnitsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "avatar_dynamodb_table_consumed_units",
		Help: "The dynamo db consumed unit",
	},
		[]string{"request", "table", "tag"},
	)
}

// TODO: make metrics track class to handle all metrics tracking
func LogConsumedUnits(tableName, logTag, requestType string, unit *types.ConsumedCapacity) {
	if unit != nil {
		consumedUnitsCounter.With(prometheus.Labels{"table": tableName, "request": requestType, "tag": logTag}).Add(*unit.CapacityUnits)
	}
}

func GetDynamoHttpClient() *awshttp.BuildableClient {
	return dynamoHttpClient
}
