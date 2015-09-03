package cfroutes

import (
	"encoding/json"

	"github.com/cloudfoundry-incubator/bbs/models"
	"github.com/cloudfoundry-incubator/receptor"
)

const CF_ROUTER = "cf-router"

type CFRoutes []CFRoute

type CFRoute struct {
	Hostnames []string `json:"hostnames"`
	Port      uint16   `json:"port"`
}

func (c CFRoutes) RoutingInfo() receptor.RoutingInfo {
	data, _ := json.Marshal(c)
	routingInfo := json.RawMessage(data)
	return receptor.RoutingInfo{
		CF_ROUTER: &routingInfo,
	}
}

func CFRoutesFromRoutingInfo(routingInfo receptor.RoutingInfo) (CFRoutes, error) {
	if routingInfo == nil {
		return nil, nil
	}

	data, found := routingInfo[CF_ROUTER]
	if !found {
		return nil, nil
	}

	if data == nil {
		return nil, nil
	}

	routes := CFRoutes{}
	err := json.Unmarshal(*data, &routes)

	return routes, err
}

// Temporary methods to get around the fact that route emitter and
// nsync work to remove the receptor are going on in parallel.
// This should be deleted by whichever track finishes first
func (c CFRoutes) TemporaryRoutingInfo() *models.Routes {
	data, _ := json.Marshal(c)
	routingInfo := json.RawMessage(data)
	return &models.Routes{
		CF_ROUTER: &routingInfo,
	}
}

func TemporaryCFRoutesFromRoutingInfo(routingInfo *models.Routes) (CFRoutes, error) {
	if routingInfo == nil {
		return nil, nil
	}

	routes := *routingInfo
	data, found := routes[CF_ROUTER]
	if !found {
		return nil, nil
	}

	if data == nil {
		return nil, nil
	}

	cfRoutes := CFRoutes{}
	err := json.Unmarshal(*data, &cfRoutes)

	return cfRoutes, err
}
