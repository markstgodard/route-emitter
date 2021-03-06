package routing_table

import (
	"errors"

	"code.cloudfoundry.org/bbs/models"
	"github.com/cloudfoundry-incubator/routing-info/cfroutes"
)

type RoutesByRoutingKey map[RoutingKey]Routes
type EndpointsByRoutingKey map[RoutingKey][]Endpoint

func RoutesByRoutingKeyFromSchedulingInfos(schedulingInfos []*models.DesiredLRPSchedulingInfo) RoutesByRoutingKey {
	routesByRoutingKey := RoutesByRoutingKey{}
	for _, desired := range schedulingInfos {
		routes, err := cfroutes.CFRoutesFromRoutingInfo(desired.Routes)
		if err == nil && len(routes) > 0 {
			for _, cfRoute := range routes {
				key := RoutingKey{ProcessGuid: desired.ProcessGuid, ContainerPort: cfRoute.Port}
				routesByRoutingKey[key] = Routes{
					Hostnames:       cfRoute.Hostnames,
					LogGuid:         desired.LogGuid,
					RouteServiceUrl: cfRoute.RouteServiceUrl,
				}
			}
		}
	}

	return routesByRoutingKey
}

func EndpointsByRoutingKeyFromActuals(actuals []*ActualLRPRoutingInfo) EndpointsByRoutingKey {
	endpointsByRoutingKey := EndpointsByRoutingKey{}
	for _, actual := range actuals {
		endpoints, err := EndpointsFromActual(actual)
		if err != nil {
			continue
		}

		for containerPort, endpoint := range endpoints {
			key := RoutingKey{ProcessGuid: actual.ActualLRP.ProcessGuid, ContainerPort: containerPort}
			endpointsByRoutingKey[key] = append(endpointsByRoutingKey[key], endpoint)
		}
	}

	return endpointsByRoutingKey
}

func EndpointsFromActual(actualLRPInfo *ActualLRPRoutingInfo) (map[uint32]Endpoint, error) {
	endpoints := map[uint32]Endpoint{}
	actual := actualLRPInfo.ActualLRP

	if len(actual.Ports) == 0 {
		return endpoints, errors.New("missing ports")
	}

	for _, portMapping := range actual.Ports {
		if portMapping != nil {
			endpoint := Endpoint{
				InstanceGuid:  actual.InstanceGuid,
				Host:          actual.Address,
				Domain:        actual.Domain,
				Port:          portMapping.HostPort,
				ContainerPort: portMapping.ContainerPort,
				Evacuating:    actualLRPInfo.Evacuating,
			}
			endpoints[portMapping.ContainerPort] = endpoint
		}
	}

	return endpoints, nil
}

func RoutingKeysFromActual(actual *models.ActualLRP) []RoutingKey {
	keys := []RoutingKey{}
	for _, portMapping := range actual.Ports {
		if portMapping != nil {
			keys = append(keys, RoutingKey{ProcessGuid: actual.ProcessGuid, ContainerPort: uint32(portMapping.ContainerPort)})
		}
	}

	return keys
}

func RoutingKeysFromSchedulingInfo(schedulingInfo *models.DesiredLRPSchedulingInfo) []RoutingKey {
	keys := []RoutingKey{}

	routes, err := cfroutes.CFRoutesFromRoutingInfo(schedulingInfo.Routes)
	if err == nil && len(routes) > 0 {
		for _, cfRoute := range routes {
			keys = append(keys, RoutingKey{ProcessGuid: schedulingInfo.ProcessGuid, ContainerPort: cfRoute.Port})
		}
	}
	return keys
}
