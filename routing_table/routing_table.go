package routing_table

import (
	"sync"

	"github.com/cloudfoundry-incubator/receptor"
	"github.com/cloudfoundry-incubator/route-emitter/cfroutes"
)

type set map[interface{}]struct{}

func (set set) contains(value interface{}) bool {
	_, found := set[value]
	return found
}

func (set set) add(value interface{}) {
	set[value] = struct{}{}
}

//go:generate counterfeiter -o fake_routing_table/fake_routing_table.go . RoutingTable
type RoutingTable interface {
	RouteCount() int

	Swap(newTable RoutingTable) MessagesToEmit

	// TODO REMOVE ME
	RemoveRoutes(key RoutingKey, modTag receptor.ModificationTag) MessagesToEmit

	SetRoutesFromDesired(desiredLRP receptor.DesiredLRPResponse) MessagesToEmit
	UpdateRoutesFromDesired(before, after receptor.DesiredLRPResponse) MessagesToEmit

	AddEndpoint(key RoutingKey, endpoint Endpoint) MessagesToEmit
	RemoveEndpoint(key RoutingKey, endpoint Endpoint) MessagesToEmit

	AddEndpointFromActual(actualLRP receptor.ActualLRPResponse) MessagesToEmit

	MessagesToEmit() MessagesToEmit
}

type RoutingTableFactory interface {
	NewTempTable(desiredLRPs []receptor.DesiredLRPResponse, actualLRPs []receptor.ActualLRPResponse) RoutingTable
}

type noopLocker struct{}

func (noopLocker) Lock()   {}
func (noopLocker) Unlock() {}

type routingTableFactory struct{}

func NewRoutingTableFactory() RoutingTableFactory {
	return &routingTableFactory{}
}

func (factory *routingTableFactory) NewTempTable(desiredLRPs []receptor.DesiredLRPResponse, actualLRPs []receptor.ActualLRPResponse) RoutingTable {
	routes := RoutesByRoutingKeyFromDesireds(desiredLRPs)
	endpoints := EndpointsByRoutingKeyFromActuals(actualLRPs)

	entries := make(map[RoutingKey]RoutableEndpoints)

	for key, entry := range routes {
		entries[key] = RoutableEndpoints{
			Hostnames: routesAsMap(entry.Hostnames),
			LogGuid:   entry.LogGuid,
		}
	}

	for key, endpoints := range endpoints {
		entry, ok := entries[key]
		if !ok {
			entry = RoutableEndpoints{}
		}
		entry.Endpoints = EndpointsAsMap(endpoints)
		entries[key] = entry
	}

	return &routingTable{
		entries:        entries,
		Locker:         noopLocker{},
		messageBuilder: NoopMessageBuilder{},
	}
}

func NewTempTable(routes RoutesByRoutingKey, endpoints EndpointsByRoutingKey) RoutingTable {
	entries := make(map[RoutingKey]RoutableEndpoints)

	for key, entry := range routes {
		entries[key] = RoutableEndpoints{
			Hostnames: routesAsMap(entry.Hostnames),
			LogGuid:   entry.LogGuid,
		}
	}

	for key, endpoints := range endpoints {
		entry, ok := entries[key]
		if !ok {
			entry = RoutableEndpoints{}
		}
		entry.Endpoints = EndpointsAsMap(endpoints)
		entries[key] = entry
	}

	return &routingTable{
		entries:        entries,
		Locker:         noopLocker{},
		messageBuilder: NoopMessageBuilder{},
	}
}

type routingTable struct {
	entries map[RoutingKey]RoutableEndpoints
	sync.Locker
	messageBuilder MessageBuilder
}

func NewTable() RoutingTable {
	return &routingTable{
		entries:        make(map[RoutingKey]RoutableEndpoints),
		Locker:         &sync.Mutex{},
		messageBuilder: MessagesToEmitBuilder{},
	}
}

func (table *routingTable) RouteCount() int {
	table.Lock()

	count := 0
	for _, entry := range table.entries {
		count += len(entry.Hostnames)
	}

	table.Unlock()
	return count
}

func (table *routingTable) Swap(t RoutingTable) MessagesToEmit {
	messagesToEmit := MessagesToEmit{}

	newTable, ok := t.(*routingTable)
	if !ok {
		return messagesToEmit
	}
	newEntries := newTable.entries

	table.Lock()
	for _, newEntry := range newEntries {
		//always register everything on sync
		messagesToEmit = messagesToEmit.merge(table.messageBuilder.RegistrationsFor(nil, &newEntry))
	}

	for key, existingEntry := range table.entries {
		newEntry := newEntries[key]
		messagesToEmit = messagesToEmit.merge(table.messageBuilder.UnregistrationsFor(&existingEntry, &newEntry))
	}

	table.entries = newEntries
	table.Unlock()

	return messagesToEmit
}

func (table *routingTable) MessagesToEmit() MessagesToEmit {
	table.Lock()

	messagesToEmit := MessagesToEmit{}
	for _, entry := range table.entries {
		messagesToEmit = messagesToEmit.merge(table.messageBuilder.RegistrationsFor(nil, &entry))
	}

	table.Unlock()
	return messagesToEmit
}

func (table *routingTable) SetRoutesFromDesired(desiredLRP receptor.DesiredLRPResponse) MessagesToEmit {
	messagesToEmit := MessagesToEmit{}

	routingKeys := RoutingKeysFromDesired(desiredLRP)
	routes, _ := cfroutes.CFRoutesFromRoutingInfo(desiredLRP.Routes)

	for _, key := range routingKeys {
		for _, route := range routes {
			if key.ContainerPort == route.Port {
				messagesToEmit = messagesToEmit.merge(table.setRoutes(key, Routes{
					Hostnames:       route.Hostnames,
					ModificationTag: desiredLRP.ModificationTag,
					LogGuid:         desiredLRP.LogGuid,
				}))
			}
		}
	}

	return messagesToEmit
}

func (table *routingTable) UpdateRoutesFromDesired(before, after receptor.DesiredLRPResponse) MessagesToEmit {
	messagesToEmit := table.SetRoutesFromDesired(after)

	beforeRoutingKeys := RoutingKeysFromDesired(before)
	afterRoutingKeys := RoutingKeysFromDesired(after)

	routes, _ := cfroutes.CFRoutesFromRoutingInfo(after.Routes)

	afterContainerPorts := set{}
	for _, route := range routes {
		afterContainerPorts.add(route.Port)
	}

	afterRoutingKeysSet := set{}
	for _, key := range afterRoutingKeys {
		afterRoutingKeysSet.add(key)
	}

	for _, key := range beforeRoutingKeys {
		if !afterRoutingKeysSet.contains(key) || !afterContainerPorts.contains(key.ContainerPort) {
			messagesToEmit = messagesToEmit.merge(table.RemoveRoutes(key, after.ModificationTag))
		}
	}

	return messagesToEmit
}

func (table *routingTable) setRoutes(key RoutingKey, routes Routes) MessagesToEmit {
	table.Lock()
	defer table.Unlock()

	currentEntry := table.entries[key]
	if !currentEntry.ModificationTag.SucceededBy(routes.ModificationTag) {
		return MessagesToEmit{}
	}

	newEntry := currentEntry.copy()
	newEntry.Hostnames = routesAsMap(routes.Hostnames)
	newEntry.LogGuid = routes.LogGuid
	newEntry.ModificationTag = routes.ModificationTag

	table.entries[key] = newEntry

	return table.emit(key, currentEntry, newEntry)
}

func (table *routingTable) RemoveRoutes(key RoutingKey, modTag receptor.ModificationTag) MessagesToEmit {
	table.Lock()
	defer table.Unlock()

	currentEntry := table.entries[key]
	if !(currentEntry.ModificationTag.Equal(modTag) || currentEntry.ModificationTag.SucceededBy(modTag)) {
		return MessagesToEmit{}
	}

	newEntry := NewRoutableEndpoints()
	newEntry.Endpoints = currentEntry.Endpoints

	table.entries[key] = currentEntry

	return table.emit(key, currentEntry, newEntry)
}

func (table *routingTable) AddEndpointFromActual(actualLRP receptor.ActualLRPResponse) MessagesToEmit {
	// TODO test error case?
	endpoints, _ := EndpointsFromActual(actualLRP)

	messagesToEmit := MessagesToEmit{}

	for _, key := range RoutingKeysFromActual(actualLRP) {
		for _, endpoint := range endpoints {
			if key.ContainerPort == endpoint.ContainerPort {
				messagesToEmit = messagesToEmit.merge(table.AddEndpoint(key, endpoint))
			}
		}
	}
	return messagesToEmit
}

func (table *routingTable) AddEndpoint(key RoutingKey, endpoint Endpoint) MessagesToEmit {
	table.Lock()
	defer table.Unlock()

	currentEntry := table.entries[key]
	newEntry := currentEntry.copy()
	newEntry.Endpoints[endpoint.key()] = endpoint
	table.entries[key] = newEntry

	return table.emit(key, currentEntry, newEntry)
}

func (table *routingTable) RemoveEndpoint(key RoutingKey, endpoint Endpoint) MessagesToEmit {
	table.Lock()
	defer table.Unlock()

	currentEntry := table.entries[key]
	endpointKey := endpoint.key()
	currentEndpoint, ok := currentEntry.Endpoints[endpointKey]
	if !ok || !(currentEndpoint.ModificationTag.Equal(endpoint.ModificationTag) || currentEndpoint.ModificationTag.SucceededBy(endpoint.ModificationTag)) {
		return MessagesToEmit{}
	}

	newEntry := currentEntry.copy()
	delete(newEntry.Endpoints, endpointKey)
	table.entries[key] = newEntry

	return table.emit(key, currentEntry, newEntry)
}

func (table *routingTable) emit(key RoutingKey, oldEntry RoutableEndpoints, newEntry RoutableEndpoints) MessagesToEmit {
	messagesToEmit := table.messageBuilder.RegistrationsFor(&oldEntry, &newEntry)
	messagesToEmit = messagesToEmit.merge(table.messageBuilder.UnregistrationsFor(&oldEntry, &newEntry))

	return messagesToEmit
}
