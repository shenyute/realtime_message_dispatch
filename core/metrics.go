package core

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var requestQueueSizeGauge *prometheus.GaugeVec
var requestDuration *prometheus.HistogramVec
var feedRooms prometheus.Gauge
var feedJoiner prometheus.Gauge
var feedSubscriber prometheus.Gauge
var receivedDataBytes prometheus.Counter
var sentDataBytes prometheus.Counter

func init() {
	requestQueueSizeGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "avatar_request_queue_size",
		Help: "The total connections current alive now",
	}, []string{"name"})
	requestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "avatar_request_duration",
			Help: "The duration of request",
		},
		[]string{"request_type"},
	)
	feedRooms = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "avatar_total_rooms",
		Help: "The total number of feed rooms",
	})
	feedJoiner = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "avatar_total_joiner",
		Help: "The total number of feed joiner",
	})
	feedSubscriber = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "avatar_total_subscriber",
		Help: "The total number of feed subscriber",
	})
	receivedDataBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "avatar_received_data_bytes",
		Help: "The total number of real bytes send in from clients",
	})
	sentDataBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "avatar_sent_data_bytes",
		Help: "The total number of real bytes send to clients",
	})
}

func DeleteTrackQueueSize(name string) {
	requestQueueSizeGauge.Delete(prometheus.Labels{"name": name})
}

func TrackQueueSize(name string, size float64) {
	requestQueueSizeGauge.With(prometheus.Labels{"name": name}).Set(size)
}

func TrackRequestDuration(requestType string, seconds float64) {
	requestDuration.With(prometheus.Labels{"request_type": requestType}).Observe(seconds)
}

func TrackFeedRoom(num float64) {
	feedRooms.Set(num)
}

func TrackFeedJoiner(num float64, isAdd bool) {
	if isAdd {
		feedJoiner.Add(num)
	} else {
		feedJoiner.Sub(num)
	}
}

func TrackFeedSubscriber(num float64, isAdd bool) {
	if isAdd {
		feedSubscriber.Add(num)
	} else {
		feedSubscriber.Sub(num)
	}
}

func TrackReceiveData(size float64) {
	receivedDataBytes.Add(size)
}

func TrackSendData(size float64) {
	sentDataBytes.Add(size)
}
