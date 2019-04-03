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
	"regexp"
	"strconv"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"

	"fmt"

	"github.com/open-falcon/falcon-plus/common/sdk/sender"
	"github.com/open-falcon/falcon-plus/modules/aggregator/g"
	"github.com/open-falcon/falcon-plus/modules/aggregator/sdk"
	"github.com/pkg/errors"
)

func WorkerPreRun(w *Worker) (err error) {
	errCnt := 0
	err = errors.New("default")
	for errCnt < 3 && err != nil {
		if err = workerPreRun(w); err != nil {
			errCnt++
			time.Sleep(time.Duration(errCnt) * time.Second)
		}
	}
	return
}

func workerPreRun(w *Worker) error {
	item := w.ClusterItem

	debug := g.Config().Debug
	if debug {
		log.Printf("processing :%+v\n", item)
	}

	numeratorStr := cleanParam(item.Numerator)
	denominatorStr := cleanParam(item.Denominator)

	if !expressionValid(numeratorStr) || !expressionValid(denominatorStr) {
		log.Println("[W] invalid numerator or denominator", item)
		return errors.New("invalid numerator or denominator")
	}
	w.numeratorStr = numeratorStr
	w.denominatorStr = denominatorStr

	w.needComputeNumerator = needCompute(numeratorStr)
	w.needComputeDenominator = needCompute(denominatorStr)

	if !w.needComputeNumerator && !w.needComputeDenominator {
		if debug {
			log.Println("[W] no need compute", item)
		}
		return errors.New("no need compute")
	}

	w.numeratorOperands, w.numeratorOperators, w.numeratorComputeMode = parse(numeratorStr, w.needComputeNumerator)
	w.denominatorOperands, w.denominatorOperators, w.denominatorComputeMode = parse(denominatorStr, w.needComputeDenominator)

	if !operatorsValid(w.numeratorOperators) || !operatorsValid(w.denominatorOperators) {
		log.Println("[W] operators invalid", item)
		return errors.New("invalid operators")
	}

	hostnames, err := sdk.HostnamesByID(item.GroupId)
	if err != nil || len(hostnames) == 0 {
		return fmt.Errorf("invalid GroupId:%d", item.GroupId)
	}
	w.hostnames = hostnames
	return nil
}

func WorkerRun(w *Worker) {
	item := w.ClusterItem

	debug := g.Config().Debug
	if debug {
		log.Printf("processing :%+v\n", item)
	}
	now := time.Now().Unix()

	valueMap, ts, err := queryCounterLast(w.numeratorOperands, w.denominatorOperands, w.hostnames, now-int64(item.Step*2), now)
	if err != nil {
		log.Println("[E]", err, item)
		return
	}

	var numerator, denominator float64
	var validCount int

	for _, hostname := range w.hostnames {
		var numeratorVal, denominatorVal float64
		var err error

		if w.needComputeNumerator {
			numeratorVal, err = compute(w.numeratorOperands, w.numeratorOperators, w.numeratorComputeMode, hostname, valueMap)

			if debug && err != nil {
				log.Printf("[W] [hostname:%s] [numerator:%s] id:%d, err:%v", hostname, item.Numerator, item.Id, err)
			} else if debug {
				log.Printf("[D] [hostname:%s] [numerator:%s] id:%d, value:%0.4f", hostname, item.Numerator, item.Id, numeratorVal)
			}

			if err != nil {
				continue
			}
		}

		if w.needComputeDenominator {
			denominatorVal, err = compute(w.denominatorOperands, w.denominatorOperators, w.denominatorComputeMode, hostname, valueMap)

			if debug && err != nil {
				log.Printf("[W] [hostname:%s] [denominator:%s] id:%d, err:%v", hostname, item.Denominator, item.Id, err)
			} else if debug {
				log.Printf("[D] [hostname:%s] [denominator:%s] id:%d, value:%0.4f", hostname, item.Denominator, item.Id, denominatorVal)
			}

			if err != nil {
				continue
			}
		}

		if debug {
			log.Printf("[D] hostname:%s  numerator:%0.4f  denominator:%0.4f  per:%0.4f\n", hostname, numeratorVal, denominatorVal, numeratorVal/denominatorVal)
		}
		numerator += numeratorVal
		denominator += denominatorVal
		validCount += 1
	}

	if !w.needComputeNumerator {
		if w.numeratorStr == "$#" {
			numerator = float64(validCount)
		} else {
			numerator, err = strconv.ParseFloat(w.numeratorStr, 64)
			if err != nil {
				log.Printf("[E] strconv.ParseFloat(%s) fail %v, id:%d", w.numeratorStr, err, item.Id)
				return
			}
		}
	}

	if !w.needComputeDenominator {
		if w.denominatorStr == "$#" {
			denominator = float64(validCount)
		} else {
			denominator, err = strconv.ParseFloat(w.denominatorStr, 64)
			if err != nil {
				log.Printf("[E] strconv.ParseFloat(%s) fail %v, id:%d", w.denominatorStr, err, item.Id)
				return
			}
		}
	}

	result := float64(0)

	if denominator != 0 {
		result = numerator / denominator
	}

	if debug {
		log.Printf("[D] item:(%+v)  numerator:%0.4f  denominator:%0.4f  result:%0.4f\n", item, numerator, denominator, result)
	}

	if denominator == 0 && numerator != 0 {
		if debug {
			log.Println("[W] denominator == 0, id: (%v)", item.Id, item)
		}
		return
	}

	if validCount == 0 {
		if debug {
			log.Println("[W] validCount == 0, id: (%v)", item.Id, item)
		}
		return
	}

	sender.Push(item.Endpoint, item.Metric, item.Tags, result, item.DsType, int64(item.Step), ts)
}

func parse(expression string, needCompute bool) (operands []string, operators []string, computeMode string) {
	if !needCompute {
		return
	}

	// e.g. $(cpu.busy)
	// e.g. $(cpu.busy)+$(cpu.idle)-$(cpu.nice)
	// e.g. $(cpu.busy)>=80
	// e.g. ($(cpu.busy)+$(cpu.idle)-$(cpu.nice))>80

	splitCounter, _ := regexp.Compile(`[\$\(\)]+`)
	items := splitCounter.Split(expression, -1)

	count := len(items)
	for i, val := range items[1 : count-1] {
		if i%2 == 0 {
			operands = append(operands, val)
		} else {
			operators = append(operators, val)
		}
	}
	computeMode = items[count-1]

	return
}

func cleanParam(val string) string {
	val = strings.TrimSpace(val)
	val = strings.Replace(val, " ", "", -1)
	val = strings.Replace(val, "\r", "", -1)
	val = strings.Replace(val, "\n", "", -1)
	val = strings.Replace(val, "\t", "", -1)
	return val
}

// $#
// 200
// $(cpu.busy) + $(cpu.idle)
func needCompute(val string) bool {
	return strings.Contains(val, "$(")
}

func expressionValid(val string) bool {
	// use chinese character?

	if strings.Contains(val, "（") || strings.Contains(val, "）") {
		return false
	}

	if val == "$#" {
		return true
	}

	// e.g. $(cpu.busy)
	// e.g. $(cpu.busy)+$(cpu.idle)-$(cpu.nice)
	matchMode0 := `^(\$\([^\(\)]+\)[+-])*\$\([^\(\)]+\)$`
	if ok, err := regexp.MatchString(matchMode0, val); err == nil && ok {
		return true
	}

	// e.g. $(cpu.busy)>=80
	matchMode1 := `^\$\([^\(\)]+\)(>|=|<|>=|<=)\d+(\.\d+)?$`
	if ok, err := regexp.MatchString(matchMode1, val); err == nil && ok {
		return true
	}

	// e.g. ($(cpu.busy)+$(cpu.idle)-$(cpu.nice))>80
	matchMode2 := `^\((\$\([^\(\)]+\)[+-])*\$\([^\(\)]+\)\)(>|=|<|>=|<=)\d+(\.\d+)?$`
	if ok, err := regexp.MatchString(matchMode2, val); err == nil && ok {
		return true
	}

	// e.g. 纯数字
	matchMode3 := `^\d+$`
	if ok, err := regexp.MatchString(matchMode3, val); err == nil && ok {
		return true
	}

	return false
}

func operatorsValid(ops []string) bool {
	count := len(ops)
	for i := 0; i < count; i++ {
		if ops[i] != "+" && ops[i] != "-" {
			return false
		}
	}
	return true
}
