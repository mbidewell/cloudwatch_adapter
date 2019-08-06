package main

import (
	"math"
	"time"
	"net/http"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

func handleWrite(w http.ResponseWriter, r *http.Request, c *cloudwatch.CloudWatch) {
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.With("err", err).Error("Failed to read body")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		log.With("err", err).Error("Failed to decompress body")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		log.With("err", err).Error("Failed to unmarshal body")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var metrics []*cloudwatch.MetricDatum
	for _, ts := range req.Timeseries {

		metric_name := ""
		instance := ""
		var dimensions []*cloudwatch.Dimension

		for _, l := range ts.Labels {
			if l.Name == model.MetricNameLabel {
				metric_name = l.Value
			} else if l.Name == "instance" {
				instance = l.Value
			}
		}
		dimensions = append(dimensions, &cloudwatch.Dimension{
			Name: aws.String("instance"),
			Value: aws.String(instance),
		})
		for _, s := range ts.Samples {
			if(!math.IsNaN(s.Value)) {
				metrics = append(metrics, &cloudwatch.MetricDatum {
					MetricName: aws.String(metric_name),
					Value: aws.Float64(s.Value),
					Timestamp: aws.Time(time.Unix(s.Timestamp / 1000, 0)),
					Dimensions: dimensions,
				})
			}
			if len(metrics) == 20 {
				_, err := c.PutMetricData(&cloudwatch.PutMetricDataInput {
					MetricData: metrics,
					Namespace: aws.String("Prometheus"),
				})

				if  err != nil {
					log.With("err", err).Error("Cannot write Samples")
					http.Error(w, err.Error(), http.StatusInternalServerError)
				}
				metrics = nil
				time.Sleep(1000 * time.Millisecond)
			}
		}
		if metrics != nil {
			_, err := c.PutMetricData(&cloudwatch.PutMetricDataInput {
				MetricData: metrics,
				Namespace: aws.String("Prometheus"),
			})

			if  err != nil {
				log.With("err", err).Error("Cannot write Samples")
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		}
	}
}


func main() {
	sess, err := session.NewSession(&aws.Config{
	    Region: aws.String("us-east-1")},
	)

	if err != nil {
		log.With("err", err).Error("Cannot Create Session")
		return
	}
	c := cloudwatch.New(sess)


	http.HandleFunc("/write", func(w http.ResponseWriter, r *http.Request) {
		handleWrite(w, r, c)
	})

	log.Fatal(http.ListenAndServe(":9201", nil))
}
