package main

import (
	"fmt"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/open-falcon/falcon-plus/modules/api/app/model/auto_aggr"
	fp "github.com/open-falcon/falcon-plus/modules/api/app/model/falcon_portal"
	"github.com/open-falcon/falcon-plus/modules/api/app/model/graph"
	//"log"
)

func genAggregator() {
	endpointCounterList := getEndpointCounters()
	for _, endpointCounter := range endpointCounterList {
		err := genAggr(endpointCounter)
		if err != nil {
			if err != NotFoundError {
				log.Printf("gen cluster for metric(%s) fail:%v", endpointCounter.Counter, err)
				continue
			}
		} else {
			log.Printf("gen cluster for metric(%s) success", endpointCounter.Counter)
		}
		deleteEndpointCounter(endpointCounter)
	}
}

func deleteEndpointCounter(c auto_aggr.EndpointCounter) error {
	return db.AutoAggr.Table(c.TableName()).Delete(&c).Error
}

func getEndpointCounters() []auto_aggr.EndpointCounter {
	//for get right table name
	enpsHelp := auto_aggr.EndpointCounter{}
	enps := []auto_aggr.EndpointCounter{}
	db.AutoAggr.Table(enpsHelp.TableName()).Scan(&enps)
	return enps
}

func getEndpointName(id uint) (string, error) {
	ep := graph.Endpoint{}
	if err := db.Graph.Table(ep.TableName()).First(&ep, id).Error; err != nil {
		return "", fmt.Errorf("get endpoint name by id(%v) fail :", id, err)
	}
	return ep.Endpoint, nil
}

func genAggr(endpointCounter auto_aggr.EndpointCounter) error {
	ep, err := getEndpointName(uint(endpointCounter.EndpointID))
	if err != nil {
		return err
	}

	orgTags := getOrgTags(endpointCounter.Counter)
	if !filter(orgTags) {
		return fmt.Errorf("filter out tags type:%s", orgTags)
	}
	//for aggr
	if isNeedCalAggrMetric(endpointCounter.Counter) {

		grpId, grpEndpointName, err := getGrpinfo(ep)
		if err != nil {
			return err
		}
		numberator := getNumberator(endpointCounter.Counter)
		denominator := getDenominator(orgTags)
		metric := getMetric(endpointCounter.Counter)
		tags := getNewTags(orgTags)
		dstype := getDstype(orgTags)
		cluster := fp.Cluster{
			GrpId:       grpId,
			Numerator:   numberator,
			Denominator: denominator,
			Endpoint:    grpEndpointName,
			Metric:      metric,
			Tags:        tags,
			DsType:      dstype,
			Step:        endpointCounter.Step,
			Creator:     autoUser,
		}

		if err := addCluster(cluster); err != nil {
			log.Printf("addCluster for aggr fail: %s. %v", err, cluster)
		} else {
			log.Printf("addCluster for aggr ok:%v", cluster)
		}
	}

	//for error ratio
	if isNeedCalErrorMetric(endpointCounter.Counter) {
		grpId, grpEndpointName, err := getGrpinfo(ep + "-00") //for it's own grp
		if err != nil {
			return err
		}
		log.Printf("grpid:%d,grpEndpoint:%s,ep:%s for error_ratio", grpId, grpEndpointName, ep)
		numberator := getErrRateNumberator(endpointCounter.Counter)
		denominator := getErrRateDenominator(endpointCounter.Counter)
		metricDenominator := strings.Trim(denominator, "$()")
		if !existsMetric(metricDenominator) {
			log.Printf("addCluster for ratio ignore: %s not exists. ", metricDenominator)
			return nil
		}
		metric := getMetric(metricDenominator)
		tags := getNewTags(orgTags)
		tags = getErrRateTags(tags)
		dstype := getDstype(orgTags)
		cluster := fp.Cluster{
			GrpId:       grpId,
			Numerator:   numberator,
			Denominator: denominator,
			Endpoint:    grpEndpointName,
			Metric:      metric,
			Tags:        tags,
			DsType:      dstype,
			Step:        endpointCounter.Step,
			Creator:     autoUser,
		}

		if err := addCluster(cluster); err != nil {
			log.Printf("addCluster for ratio fail: %s. %v", err, cluster)
		} else {
			log.Printf("addCluster for ratio ok:%v", cluster)
		}
	}
	return nil
}

func isNeedCalAggrMetric(counter string) bool {
	return strings.Contains(counter, "need_aggr")
}
func isNeedCalErrorMetric(counter string) bool {
	log.Printf("isNeedCalErrorMetric:%v, 1", counter)
	if !strings.HasPrefix(counter, "error.") || strings.Contains(counter, "need_aggr") {
		return false
	}
	log.Printf("isNeedCalErrorMetric:%v, 2", counter)
	list := strings.Split(counter, ",")
	for _, tag := range list {
		valueType := getValue(tag)
		switch valueType {
		case "rate":
			log.Printf("isNeedCalErrorMetric:%v, 3", counter)
			return true
		}
	}
	log.Printf("isNeedCalErrorMetric:%v, 4", counter)
	return false
}

func existsMetric(m string) bool {
	ep := graph.EndpointCounter{Counter: m}
	if err := db.Graph.Table(ep.TableName()).Where(&ep).First(&ep).Error; err != nil {
		log.Printf("find %s in graph fail:%v, ", m, err)
		return false
	}
	return true

}

func addCluster(c fp.Cluster) error {
	return db.Falcon.Table(c.TableName()).FirstOrCreate(&c, c).Error
}

func getGrpinfo(endpoint string) (int64, string, error) {
	grpName := getLeaderName(endpoint)
	grpId, err := findGrpByLeader(grpName, false)
	if err != nil {
		return -1, "", err
	}
	return grpId, grpName, nil
}

func getNumberator(counter string) string {
	return "$(" + counter + ")"
}

func getValue(str string) string {
	kv := strings.Split(str, "=")
	return strings.TrimSpace(kv[len(kv)-1])
}

func getErrRateNumberator(counter string) string {
	return "$(" + counter + ")"
}

func getErrRateDenominator(counter string) string {
	return "$(" + strings.Replace(strings.TrimPrefix(counter, "error."), "metricType=meter", "metricType=timer", -1) + ")"
}

func filter(orgTags string) bool {
	list := strings.Split(orgTags, ",")
	for _, tag := range list {
		valueType := getValue(tag)
		switch valueType {
		case "rate15", "rate5", "ratemean":
			return false
		}
	}
	return true
}

func getDenominator(orgTags string) string {
	list := strings.Split(orgTags, ",")
	for _, tag := range list {
		valueType := getValue(tag)
		switch valueType {
		case "75%", "95%", "99%", "max", "mean", "median", "min":
			return "$#"
		case "count", "interval_count", "rate", "rate1", "rate15", "rate5", "ratemean":
			return "1"
		}
	}
	log.Printf("getDenominator fail:%s ", orgTags)
	return "1"
}

func getErrRateTags(tags string) string {
	return strings.Replace(tags, "valueType=rate", "valueType=error_ratio", -1)
}

func getMetric(counter string) string {
	list := strings.Split(counter, "/")
	return strings.Join(list[0:len(list)-1], "/")
}

func getOrgTags(counter string) string {
	list := strings.Split(counter, "/")
	if len(list) < 2 {
		return ""
	}
	return list[len(list)-1]
}

func getNewTags(orgTags string) string {
	list := strings.Split(orgTags, ",")
	newList := []string{}
	for _, v := range list {
		if strings.Contains(v, "need_aggr") {
			continue
		}
		newList = append(newList, v)
	}
	return strings.Join(newList, ",")
}

func getDstype(orgTags string) string {
	/*
		list := strings.Split(orgTags, ",")
		for _, v := range list {
			if strings.TrimSpace(v) == "valueType=rate" {
			    return "COUNTER"
			}
		}
	*/
	return "GAUGE"
}
