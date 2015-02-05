package routing_table

import (
	"github.com/cloudfoundry-incubator/receptor"
	"github.com/cloudfoundry-incubator/route-emitter/cfroutes"
)

type RoutesByRoutingKey map[RoutingKey]Routes
type EndpointsByRoutingKey map[RoutingKey][]Endpoint

func RoutesByRoutingKeyFromDesireds(desireds []receptor.DesiredLRPResponse) RoutesByRoutingKey {
	routesByRoutingKey := RoutesByRoutingKey{}
	for _, desired := range desireds {
		routes, err := cfroutes.CFRoutesFromRoutingInfo(desired.Routes)
		if err == nil && len(routes) > 0 {
			for _, cfRoute := range routes {
				key := RoutingKey{ProcessGuid: desired.ProcessGuid, ContainerPort: cfRoute.Port}
				routesByRoutingKey[key] = Routes{
					URIs:    cfRoute.Hostnames,
					LogGuid: desired.LogGuid,
				}
			}
		}
	}

	return routesByRoutingKey
}

func EndpointsByRoutingKeyFromActuals(actuals []receptor.ActualLRPResponse) EndpointsByRoutingKey {
	endpointsByRoutingKey := EndpointsByRoutingKey{}
	for _, actual := range actuals {
		if len(actual.Ports) == 0 {
			continue
		}

		endpoints := endpointsFromActual(actual)
		for containerPort, endpoint := range endpoints {
			key := RoutingKey{ProcessGuid: actual.ProcessGuid, ContainerPort: containerPort}
			endpointsByRoutingKey[key] = append(endpointsByRoutingKey[key], endpoint)
		}
	}

	return endpointsByRoutingKey
}

func RoutingKeysFromDesired(desired receptor.DesiredLRPResponse) []RoutingKey {
	keys := []RoutingKey{}
	for _, containerPort := range desired.Ports {
		keys = append(keys, RoutingKey{ProcessGuid: desired.ProcessGuid, ContainerPort: containerPort})
	}

	return keys
}

func endpointsFromActual(actual receptor.ActualLRPResponse) map[uint16]Endpoint {
	endpoints := map[uint16]Endpoint{}
	for _, portMapping := range actual.Ports {
		endpoint := Endpoint{
			InstanceGuid:  actual.InstanceGuid,
			Host:          actual.Address,
			Port:          portMapping.HostPort,
			ContainerPort: portMapping.ContainerPort,
		}
		endpoints[portMapping.ContainerPort] = endpoint
	}

	return endpoints
}
