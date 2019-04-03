// Copyright 2017 Xiaomi, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cron

import (
	"time"

	log "github.com/Sirupsen/logrus"

	"fmt"
	"math/rand"

	"sync"

	"github.com/open-falcon/falcon-plus/modules/aggregator/g"
)

type Worker struct {
	Ticker      *time.Ticker
	ClusterItem *g.Cluster
	Quit        chan struct{}

	numeratorOperands      []string
	numeratorOperators     []string
	numeratorComputeMode   string
	denominatorOperands    []string
	denominatorOperators   []string
	denominatorComputeMode string
	hostnames              []string
	needComputeNumerator   bool
	needComputeDenominator bool
	numeratorStr           string
	denominatorStr         string
}

func NewWorker(ci *g.Cluster) Worker {
	w := Worker{}
	w.Quit = make(chan struct{})
	w.ClusterItem = ci
	return w
}

func (this Worker) Start() error {
	if err := WorkerPreRun(&this); err != nil {
		log.Printf("[E] WorkerPreRun fail(%+v),err:%s", this.ClusterItem, err)
		return err
	}

	go func() {
		fmt.Printf("I sleep for %d seconds then start.(%+v)\n", rand.Intn(this.ClusterItem.Step), this.ClusterItem)
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(1000*this.ClusterItem.Step)))

		this.Ticker = time.NewTicker(time.Duration(this.ClusterItem.Step) * time.Second)
		for {
			select {
			case <-this.Ticker.C:
				WorkerRun(&this)
			case <-this.Quit:
				if g.Config().Debug {
					log.Println("[I] drop worker", this.ClusterItem)
				}
				this.Ticker.Stop()
				return
			}
		}
	}()
	return nil
}

func (this Worker) Drop() {
	close(this.Quit)
}

var WorkersMap sync.Map

func deleteNoUseWorker(m map[string]*g.Cluster) {
	del := []string{}
	WorkersMap.Range(func(k, v interface{}) bool {
		key := k.(string)
		worker := v.(*Worker)
		if _, ok := m[key]; !ok {
			worker.Drop()
			del = append(del, key)
		}
		return true
	})

	for _, key := range del {
		WorkersMap.Delete(key)
	}
}

func createWorkerIfNeed(m map[string]*g.Cluster) {
	for key, item := range m {
		if _, ok := WorkersMap.Load(key); !ok {
			if item.Step <= 0 {
				log.Println("[W] invalid cluster(step <= 0):", item)
				continue
			}
			//go func() {
			worker := NewWorker(item)
			if worker.Start() == nil {
				WorkersMap.Store(key, &worker)
			}
			//}()
		}
	}
}
