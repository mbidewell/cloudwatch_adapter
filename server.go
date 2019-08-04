package main

import (
	"net/http"
	"io/ioutil"

	"github.com/gocql/gocql"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

func handleWrite(w http.ResponseWriter, r *http.Request, session *gocql.Session) {
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

	for _, ts := range req.Timeseries {
		metric := make(model.Metric, len(ts.Labels))
		metric_name := ""

		for _, l := range ts.Labels {
			if l.Name == labels.MetricNameLabel {
				metric_name = l.Value
			} else {
				metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
			}
		}

		for _, s := range ts.Samples {
			err := session.Query("insert into metrics(timestamp,name,labels_hash, labels, value) values(?,?,?,?,?)",
														s.Timestamp,
														metric_name,
														metric.Fingerprint().String(),
														metric,
														s.Value).Exec()
			if  err != nil {
				log.With("err", err).Error("Cannot write Sample")
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		}
	}
}


func main() {
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "prometheus"
	session, err := cluster.CreateSession()

	if err != nil {
		log.With("err", err).Error("Cannot Connect to DB")
		return
	}

	http.HandleFunc("/write", func(w http.ResponseWriter, r *http.Request) {
		handleWrite(w, r, session)
	})

	log.Fatal(http.ListenAndServe(":9201", nil))
}
