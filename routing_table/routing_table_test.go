package routing_table_test

import (
	"github.com/cloudfoundry-incubator/receptor"
	"github.com/cloudfoundry-incubator/route-emitter/cfroutes"
	"github.com/cloudfoundry-incubator/route-emitter/routing_table"
	"github.com/cloudfoundry-incubator/runtime-schema/models"

	. "github.com/cloudfoundry-incubator/route-emitter/routing_table/matchers"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("RoutingTable", func() {
	var (
		table          routing_table.RoutingTable
		messagesToEmit routing_table.MessagesToEmit
	)

	key := routing_table.RoutingKey{ProcessGuid: "some-process-guid", ContainerPort: 8080}

	hostname1 := "foo.example.com"
	hostname2 := "bar.example.com"
	hostname3 := "baz.example.com"

	olderTag := receptor.ModificationTag{Epoch: "abc", Index: 0}
	currentTag := receptor.ModificationTag{Epoch: "abc", Index: 1}
	newerTag := receptor.ModificationTag{Epoch: "def", Index: 0}

	endpoint1 := routing_table.Endpoint{InstanceGuid: "ig-1", Host: "1.1.1.1", Port: 11, ContainerPort: 8080, Evacuating: false, ModificationTag: currentTag}
	endpoint2 := routing_table.Endpoint{InstanceGuid: "ig-2", Host: "2.2.2.2", Port: 22, ContainerPort: 8080, Evacuating: false, ModificationTag: currentTag}
	endpoint3 := routing_table.Endpoint{InstanceGuid: "ig-3", Host: "3.3.3.3", Port: 33, ContainerPort: 8080, Evacuating: false, ModificationTag: currentTag}

	evacuating1 := routing_table.Endpoint{InstanceGuid: "ig-1", Host: "1.1.1.1", Port: 11, ContainerPort: 8080, Evacuating: true, ModificationTag: currentTag}

	logGuid := "some-log-guid"

	getDesiredLRP := func(key routing_table.RoutingKey, routes routing_table.Routes) receptor.DesiredLRPResponse {
		var desiredLRP receptor.DesiredLRPResponse
		desiredLRP.ProcessGuid = key.ProcessGuid
		desiredLRP.Ports = []uint16{key.ContainerPort}
		desiredLRP.LogGuid = routes.LogGuid
		desiredLRP.ModificationTag = routes.ModificationTag
		cfRoutes := cfroutes.CFRoutes{
			cfroutes.CFRoute{
				Hostnames: routes.Hostnames,
				Port:      key.ContainerPort,
			},
		}
		desiredLRP.Routes = cfRoutes.RoutingInfo()
		return desiredLRP
	}

	getActualLRP := func(key routing_table.RoutingKey, endpoint routing_table.Endpoint) receptor.ActualLRPResponse {
		actualLRP := receptor.ActualLRPResponse{
			ProcessGuid:     key.ProcessGuid,
			InstanceGuid:    endpoint.InstanceGuid,
			Address:         endpoint.Host,
			Evacuating:      endpoint.Evacuating,
			ModificationTag: endpoint.ModificationTag,
			Ports:           []receptor.PortMapping{{ContainerPort: endpoint.ContainerPort, HostPort: endpoint.Port}},
		}
		return actualLRP
	}

	BeforeEach(func() {
		table = routing_table.NewTable()
	})

	Describe("Swap", func() {
		Context("when a new routing key arrives", func() {
			Context("when the routing key has both routes and endpoints", func() {
				BeforeEach(func() {
					tempTable := routing_table.NewTempTable(
						routing_table.RoutesByRoutingKey{key: routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}},
						routing_table.EndpointsByRoutingKey{key: {endpoint1, endpoint2}},
					)

					messagesToEmit = table.Swap(tempTable)
				})

				It("emits registrations for each pairing", func() {
					expected := routing_table.MessagesToEmit{
						RegistrationMessages: []routing_table.RegistryMessage{
							routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
							routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
						},
					}
					Expect(messagesToEmit).To(MatchMessagesToEmit(expected))
				})
			})

			Context("when the process only has routes", func() {
				BeforeEach(func() {
					tempTable := routing_table.NewTempTable(
						routing_table.RoutesByRoutingKey{key: routing_table.Routes{Hostnames: []string{hostname1}, LogGuid: logGuid}},
						routing_table.EndpointsByRoutingKey{},
					)
					messagesToEmit = table.Swap(tempTable)
				})

				It("should not emit a registration", func() {
					Expect(messagesToEmit).To(BeZero())
				})

				Context("when the endpoints subsequently arrive", func() {
					BeforeEach(func() {
						tempTable := routing_table.NewTempTable(
							routing_table.RoutesByRoutingKey{key: routing_table.Routes{Hostnames: []string{hostname1}, LogGuid: logGuid}},
							routing_table.EndpointsByRoutingKey{key: {endpoint1}},
						)
						messagesToEmit = table.Swap(tempTable)
					})

					It("emits registrations for each pairing", func() {
						expected := routing_table.MessagesToEmit{
							RegistrationMessages: []routing_table.RegistryMessage{
								routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname1}, LogGuid: logGuid}),
							},
						}
						Expect(messagesToEmit).To(MatchMessagesToEmit(expected))
					})
				})

				Context("when the routing key subsequently disappears", func() {
					BeforeEach(func() {
						tempTable := routing_table.NewTempTable(
							routing_table.RoutesByRoutingKey{},
							routing_table.EndpointsByRoutingKey{},
						)
						messagesToEmit = table.Swap(tempTable)
					})

					It("emits nothing", func() {
						Expect(messagesToEmit).To(BeZero())
					})
				})
			})

			Context("when the process only has endpoints", func() {
				BeforeEach(func() {
					tempTable := routing_table.NewTempTable(
						routing_table.RoutesByRoutingKey{},
						routing_table.EndpointsByRoutingKey{key: {endpoint1}},
					)
					messagesToEmit = table.Swap(tempTable)
				})

				It("should not emit a registration", func() {
					Expect(messagesToEmit).To(BeZero())
				})

				Context("when the routes subsequently arrive", func() {
					BeforeEach(func() {
						tempTable := routing_table.NewTempTable(
							routing_table.RoutesByRoutingKey{key: routing_table.Routes{Hostnames: []string{hostname1}, LogGuid: logGuid}},
							routing_table.EndpointsByRoutingKey{key: {endpoint1}},
						)
						messagesToEmit = table.Swap(tempTable)
					})

					It("emits registrations for each pairing", func() {
						expected := routing_table.MessagesToEmit{
							RegistrationMessages: []routing_table.RegistryMessage{
								routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname1}, LogGuid: logGuid}),
							},
						}
						Expect(messagesToEmit).To(MatchMessagesToEmit(expected))
					})
				})

				Context("when the endpoint subsequently disappears", func() {
					BeforeEach(func() {
						tempTable := routing_table.NewTempTable(
							routing_table.RoutesByRoutingKey{},
							routing_table.EndpointsByRoutingKey{},
						)
						messagesToEmit = table.Swap(tempTable)
					})

					It("emits nothing", func() {
						Expect(messagesToEmit).To(BeZero())
					})
				})
			})
		})

		Context("when there is an existing routing key", func() {
			BeforeEach(func() {
				tempTable := routing_table.NewTempTable(
					routing_table.RoutesByRoutingKey{key: routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}},
					routing_table.EndpointsByRoutingKey{key: {endpoint1, endpoint2}},
				)
				table.Swap(tempTable)
			})

			Context("when nothing changes", func() {
				BeforeEach(func() {
					tempTable := routing_table.NewTempTable(
						routing_table.RoutesByRoutingKey{key: routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}},
						routing_table.EndpointsByRoutingKey{key: {endpoint1, endpoint2}},
					)
					messagesToEmit = table.Swap(tempTable)
				})

				It("emits all registrations and no unregisration", func() {
					expected := routing_table.MessagesToEmit{
						RegistrationMessages: []routing_table.RegistryMessage{
							routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
							routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
						},
					}
					Expect(messagesToEmit).To(MatchMessagesToEmit(expected))
				})
			})

			Context("when the routing key gets new routes", func() {
				BeforeEach(func() {
					tempTable := routing_table.NewTempTable(
						routing_table.RoutesByRoutingKey{key: routing_table.Routes{Hostnames: []string{hostname1, hostname2, hostname3}, LogGuid: logGuid}},
						routing_table.EndpointsByRoutingKey{key: {endpoint1, endpoint2}},
					)
					messagesToEmit = table.Swap(tempTable)
				})

				It("emits all registrations and no unregisration", func() {
					expected := routing_table.MessagesToEmit{
						RegistrationMessages: []routing_table.RegistryMessage{
							routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname1, hostname2, hostname3}, LogGuid: logGuid}),
							routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname1, hostname2, hostname3}, LogGuid: logGuid}),
						},
					}
					Expect(messagesToEmit).To(MatchMessagesToEmit(expected))
				})
			})

			Context("when the routing key gets new endpoints", func() {
				BeforeEach(func() {
					tempTable := routing_table.NewTempTable(
						routing_table.RoutesByRoutingKey{key: routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}},
						routing_table.EndpointsByRoutingKey{key: {endpoint1, endpoint2, endpoint3}},
					)
					messagesToEmit = table.Swap(tempTable)
				})

				It("emits all registrations and no unregisration", func() {
					expected := routing_table.MessagesToEmit{
						RegistrationMessages: []routing_table.RegistryMessage{
							routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
							routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
							routing_table.RegistryMessageFor(endpoint3, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
						},
					}
					Expect(messagesToEmit).To(MatchMessagesToEmit(expected))
				})
			})

			Context("when the routing key gets a new evacuating endpoint", func() {
				BeforeEach(func() {
					tempTable := routing_table.NewTempTable(
						routing_table.RoutesByRoutingKey{key: routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}},
						routing_table.EndpointsByRoutingKey{key: {endpoint1, endpoint2, evacuating1}},
					)
					messagesToEmit = table.Swap(tempTable)
				})

				It("emits all registrations and no unregisration", func() {
					expected := routing_table.MessagesToEmit{
						RegistrationMessages: []routing_table.RegistryMessage{
							routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
							routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
							routing_table.RegistryMessageFor(evacuating1, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
						},
					}
					Expect(messagesToEmit).To(MatchMessagesToEmit(expected))
				})
			})

			Context("when the routing key has an evacuating and instance endpoint", func() {
				BeforeEach(func() {
					tempTable := routing_table.NewTempTable(
						routing_table.RoutesByRoutingKey{key: routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}},
						routing_table.EndpointsByRoutingKey{key: {endpoint1, endpoint2, evacuating1}},
					)
					table.Swap(tempTable)

					tempTable = routing_table.NewTempTable(
						routing_table.RoutesByRoutingKey{key: routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}},
						routing_table.EndpointsByRoutingKey{key: {endpoint2, evacuating1}},
					)
					messagesToEmit = table.Swap(tempTable)
				})

				It("should not emit an unregistration ", func() {
					expected := routing_table.MessagesToEmit{
						RegistrationMessages: []routing_table.RegistryMessage{
							routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
							routing_table.RegistryMessageFor(evacuating1, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
						},
					}
					Expect(messagesToEmit).To(MatchMessagesToEmit(expected))
				})
			})

			Context("when the routing key gets new routes and endpoints", func() {
				BeforeEach(func() {
					tempTable := routing_table.NewTempTable(
						routing_table.RoutesByRoutingKey{key: routing_table.Routes{Hostnames: []string{hostname1, hostname2, hostname3}, LogGuid: logGuid}},
						routing_table.EndpointsByRoutingKey{key: {endpoint1, endpoint2, endpoint3}},
					)
					messagesToEmit = table.Swap(tempTable)
				})

				It("emits all registrations and no unregisration", func() {
					expected := routing_table.MessagesToEmit{
						RegistrationMessages: []routing_table.RegistryMessage{
							routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname1, hostname2, hostname3}, LogGuid: logGuid}),
							routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname1, hostname2, hostname3}, LogGuid: logGuid}),
							routing_table.RegistryMessageFor(endpoint3, routing_table.Routes{Hostnames: []string{hostname1, hostname2, hostname3}, LogGuid: logGuid}),
						},
					}
					Expect(messagesToEmit).To(MatchMessagesToEmit(expected))
				})
			})

			Context("when the routing key loses routes", func() {
				BeforeEach(func() {
					tempTable := routing_table.NewTempTable(
						routing_table.RoutesByRoutingKey{key: routing_table.Routes{Hostnames: []string{hostname1}, LogGuid: logGuid}},
						routing_table.EndpointsByRoutingKey{key: {endpoint1, endpoint2}},
					)
					messagesToEmit = table.Swap(tempTable)
				})

				It("emits all registrations and the relevant unregisrations", func() {
					expected := routing_table.MessagesToEmit{
						RegistrationMessages: []routing_table.RegistryMessage{
							routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname1}, LogGuid: logGuid}),
							routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname1}, LogGuid: logGuid}),
						},
						UnregistrationMessages: []routing_table.RegistryMessage{
							routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname2}, LogGuid: logGuid}),
							routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname2}, LogGuid: logGuid}),
						},
					}
					Expect(messagesToEmit).To(MatchMessagesToEmit(expected))
				})
			})

			Context("when the routing key loses endpoints", func() {
				BeforeEach(func() {
					tempTable := routing_table.NewTempTable(
						routing_table.RoutesByRoutingKey{key: routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}},
						routing_table.EndpointsByRoutingKey{key: {endpoint1}},
					)
					messagesToEmit = table.Swap(tempTable)
				})

				It("emits all registrations and the relevant unregisrations", func() {
					expected := routing_table.MessagesToEmit{
						RegistrationMessages: []routing_table.RegistryMessage{
							routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
						},
						UnregistrationMessages: []routing_table.RegistryMessage{
							routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
						},
					}
					Expect(messagesToEmit).To(MatchMessagesToEmit(expected))
				})
			})

			Context("when the routing key loses both routes and endpoints", func() {
				BeforeEach(func() {
					tempTable := routing_table.NewTempTable(
						routing_table.RoutesByRoutingKey{key: routing_table.Routes{Hostnames: []string{hostname1}, LogGuid: logGuid}},
						routing_table.EndpointsByRoutingKey{key: {endpoint1}},
					)
					messagesToEmit = table.Swap(tempTable)
				})

				It("emits all registrations and the relevant unregisrations", func() {
					expected := routing_table.MessagesToEmit{
						RegistrationMessages: []routing_table.RegistryMessage{
							routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname1}, LogGuid: logGuid}),
						},
						UnregistrationMessages: []routing_table.RegistryMessage{
							routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname2}, LogGuid: logGuid}),
							routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
						},
					}
					Expect(messagesToEmit).To(MatchMessagesToEmit(expected))
				})
			})

			Context("when the routing key gains routes but loses endpoints", func() {
				BeforeEach(func() {
					tempTable := routing_table.NewTempTable(
						routing_table.RoutesByRoutingKey{key: routing_table.Routes{Hostnames: []string{hostname1, hostname2, hostname3}, LogGuid: logGuid}},
						routing_table.EndpointsByRoutingKey{key: {endpoint1}},
					)
					messagesToEmit = table.Swap(tempTable)
				})

				It("emits all registrations and the relevant unregisrations", func() {
					expected := routing_table.MessagesToEmit{
						RegistrationMessages: []routing_table.RegistryMessage{
							routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname1, hostname2, hostname3}, LogGuid: logGuid}),
						},
						UnregistrationMessages: []routing_table.RegistryMessage{
							routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
						},
					}
					Expect(messagesToEmit).To(MatchMessagesToEmit(expected))
				})
			})

			Context("when the routing key loses routes but gains endpoints", func() {
				BeforeEach(func() {
					tempTable := routing_table.NewTempTable(
						routing_table.RoutesByRoutingKey{key: routing_table.Routes{Hostnames: []string{hostname1}, LogGuid: logGuid}},
						routing_table.EndpointsByRoutingKey{key: {endpoint1, endpoint2, endpoint3}},
					)
					messagesToEmit = table.Swap(tempTable)
				})

				It("emits all registrations and the relevant unregisrations", func() {
					expected := routing_table.MessagesToEmit{
						RegistrationMessages: []routing_table.RegistryMessage{
							routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname1}, LogGuid: logGuid}),
							routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname1}, LogGuid: logGuid}),
							routing_table.RegistryMessageFor(endpoint3, routing_table.Routes{Hostnames: []string{hostname1}, LogGuid: logGuid}),
						},
						UnregistrationMessages: []routing_table.RegistryMessage{
							routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname2}, LogGuid: logGuid}),
							routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname2}, LogGuid: logGuid}),
						},
					}
					Expect(messagesToEmit).To(MatchMessagesToEmit(expected))
				})
			})

			Context("when the routing key disappears entirely", func() {
				BeforeEach(func() {
					tempTable := routing_table.NewTempTable(
						routing_table.RoutesByRoutingKey{},
						routing_table.EndpointsByRoutingKey{},
					)
					messagesToEmit = table.Swap(tempTable)
				})

				It("should unregister the missing guids", func() {
					expected := routing_table.MessagesToEmit{
						UnregistrationMessages: []routing_table.RegistryMessage{
							routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
							routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
						},
					}
					Expect(messagesToEmit).To(MatchMessagesToEmit(expected))
				})
			})

			Describe("edge cases", func() {
				Context("when the original registration had no routes, and then the routing key loses endpoints", func() {
					BeforeEach(func() {
						//override previous set up
						tempTable := routing_table.NewTempTable(
							routing_table.RoutesByRoutingKey{},
							routing_table.EndpointsByRoutingKey{key: {endpoint1, endpoint2}},
						)
						table.Swap(tempTable)

						tempTable = routing_table.NewTempTable(
							routing_table.RoutesByRoutingKey{key: {}},
							routing_table.EndpointsByRoutingKey{key: {endpoint1}},
						)
						messagesToEmit = table.Swap(tempTable)
					})

					It("emits nothing", func() {
						Expect(messagesToEmit).To(BeZero())
					})
				})

				Context("when the original registration had no endpoints, and then the routing key loses a route", func() {
					BeforeEach(func() {
						//override previous set up
						tempTable := routing_table.NewTempTable(
							routing_table.RoutesByRoutingKey{key: routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}},
							routing_table.EndpointsByRoutingKey{},
						)
						table.Swap(tempTable)

						tempTable = routing_table.NewTempTable(
							routing_table.RoutesByRoutingKey{key: routing_table.Routes{Hostnames: []string{hostname1}, LogGuid: logGuid}},
							routing_table.EndpointsByRoutingKey{},
						)
						messagesToEmit = table.Swap(tempTable)
					})

					It("emits nothing", func() {
						Expect(messagesToEmit).To(BeZero())
					})
				})
			})
		})
	})

	Describe("Processing deltas", func() {
		Context("when the table is empty", func() {
			Context("When setting routes", func() {
				It("emits nothing", func() {
					desiredLRP := getDesiredLRP(key, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid, ModificationTag: currentTag})
					messagesToEmit = table.SetRoutesFromDesired(desiredLRP)
					Expect(messagesToEmit).To(BeZero())
				})
			})

			Context("when removing routes", func() {
				It("emits nothing", func() {
					messagesToEmit = table.RemoveRoutes(key, currentTag)
					Expect(messagesToEmit).To(BeZero())
				})
			})

			Context("when adding/updating endpoints", func() {
				It("emits nothing", func() {
					actualLRP := getActualLRP(key, endpoint1)
					messagesToEmit = table.AddEndpointFromActual(actualLRP)
					Expect(messagesToEmit).To(BeZero())
				})
			})

			Context("when removing endpoints", func() {
				It("emits nothing", func() {
					messagesToEmit = table.RemoveEndpoint(key, endpoint1)
					Expect(messagesToEmit).To(BeZero())
				})
			})
		})

		Context("when there are both endpoints and routes in the table", func() {
			BeforeEach(func() {
				desiredLRP := getDesiredLRP(key, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid, ModificationTag: currentTag})
				table.SetRoutesFromDesired(desiredLRP)
				actualLRP := getActualLRP(key, endpoint1)
				messagesToEmit = table.AddEndpointFromActual(actualLRP)

				actualLRP2 := getActualLRP(key, endpoint2)
				messagesToEmit = table.AddEndpointFromActual(actualLRP2)
			})

			Describe("SetRoutes", func() {
				It("emits nothing when the route's hostnames do not change", func() {
					desiredLRP := getDesiredLRP(key, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid, ModificationTag: currentTag})
					messagesToEmit = table.SetRoutesFromDesired(desiredLRP)
					Expect(messagesToEmit).To(BeZero())
				})

				It("emits nothing when the route's container ports do not match the key", func() {
					desiredLRP := getDesiredLRP(key, routing_table.Routes{Hostnames: []string{hostname1, hostname2, hostname3}, LogGuid: logGuid, ModificationTag: newerTag})
					cfRoutes := cfroutes.CFRoutes{
						cfroutes.CFRoute{
							Hostnames: []string{hostname1, hostname2, hostname3},
							Port:      9090,
						},
					}
					desiredLRP.Routes = cfRoutes.RoutingInfo()
					messagesToEmit = table.SetRoutesFromDesired(desiredLRP)

					Expect(messagesToEmit).To(BeZero())
				})

				It("emits nothing when a hostname is added to a route with an older tag", func() {
					desiredLRP := getDesiredLRP(key, routing_table.Routes{Hostnames: []string{hostname1, hostname2, hostname3}, LogGuid: logGuid, ModificationTag: olderTag})
					messagesToEmit = table.SetRoutesFromDesired(desiredLRP)
					Expect(messagesToEmit).To(BeZero())
				})

				It("emits registrations when a hostname is added to a route with a newer tag", func() {
					desiredLRP := getDesiredLRP(key, routing_table.Routes{Hostnames: []string{hostname1, hostname2, hostname3}, LogGuid: logGuid, ModificationTag: newerTag})
					messagesToEmit = table.SetRoutesFromDesired(desiredLRP)

					expected := routing_table.MessagesToEmit{
						RegistrationMessages: []routing_table.RegistryMessage{
							routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname1, hostname2, hostname3}, LogGuid: logGuid}),
							routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname1, hostname2, hostname3}, LogGuid: logGuid}),
						},
					}
					Expect(messagesToEmit).To(MatchMessagesToEmit(expected))
				})

				It("emits nothing when a hostname is removed from a route with an older tag", func() {
					desiredLRP := getDesiredLRP(key, routing_table.Routes{Hostnames: []string{hostname1}, LogGuid: logGuid, ModificationTag: olderTag})
					messagesToEmit = table.SetRoutesFromDesired(desiredLRP)
					Expect(messagesToEmit).To(BeZero())
				})

				It("emits registrations and unregistrations when a hostname is removed from a route with a newer tag", func() {
					desiredLRP := getDesiredLRP(key, routing_table.Routes{Hostnames: []string{hostname1}, LogGuid: logGuid, ModificationTag: newerTag})
					messagesToEmit = table.SetRoutesFromDesired(desiredLRP)

					expected := routing_table.MessagesToEmit{
						RegistrationMessages: []routing_table.RegistryMessage{
							routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname1}, LogGuid: logGuid}),
							routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname1}, LogGuid: logGuid}),
						},
						UnregistrationMessages: []routing_table.RegistryMessage{
							routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname2}, LogGuid: logGuid}),
							routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname2}, LogGuid: logGuid}),
						},
					}
					Expect(messagesToEmit).To(MatchMessagesToEmit(expected))
				})

				It("emits nothing when hostnames are added and removed from a route with an older tag", func() {
					desiredLRP := getDesiredLRP(key, routing_table.Routes{Hostnames: []string{hostname1, hostname3}, LogGuid: logGuid, ModificationTag: olderTag})
					messagesToEmit = table.SetRoutesFromDesired(desiredLRP)
					Expect(messagesToEmit).To(BeZero())
				})

				It("emits registrations and unregistrations when hostnames are added and removed from a route with a newer tag", func() {
					desiredLRP := getDesiredLRP(key, routing_table.Routes{Hostnames: []string{hostname1, hostname3}, LogGuid: logGuid, ModificationTag: newerTag})
					messagesToEmit = table.SetRoutesFromDesired(desiredLRP)

					expected := routing_table.MessagesToEmit{
						RegistrationMessages: []routing_table.RegistryMessage{
							routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname1, hostname3}, LogGuid: logGuid}),
							routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname1, hostname3}, LogGuid: logGuid}),
						},
						UnregistrationMessages: []routing_table.RegistryMessage{
							routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname2}, LogGuid: logGuid}),
							routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname2}, LogGuid: logGuid}),
						},
					}
					Expect(messagesToEmit).To(MatchMessagesToEmit(expected))
				})
			})

			Context("RemoveRoutes", func() {
				It("emits unregistrations with a newer tag", func() {
					messagesToEmit = table.RemoveRoutes(key, newerTag)

					expected := routing_table.MessagesToEmit{
						UnregistrationMessages: []routing_table.RegistryMessage{
							routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
							routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
						},
					}
					Expect(messagesToEmit).To(MatchMessagesToEmit(expected))
				})

				It("emits unregistrations with the same tag", func() {
					messagesToEmit = table.RemoveRoutes(key, newerTag)

					expected := routing_table.MessagesToEmit{
						UnregistrationMessages: []routing_table.RegistryMessage{
							routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
							routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
						},
					}
					Expect(messagesToEmit).To(MatchMessagesToEmit(expected))
				})

				It("emits nothing when the tag is older", func() {
					messagesToEmit = table.RemoveRoutes(key, olderTag)
					Expect(messagesToEmit).To(BeZero())
				})
			})

			Context("AddEndpointFromActual", func() {
				It("emits nothing when the tag is the same", func() {
					actualLRP := getActualLRP(key, endpoint1)
					messagesToEmit = table.AddEndpointFromActual(actualLRP)
					Expect(messagesToEmit).To(BeZero())
				})

				It("emits nothing when updating an endpoint with an older tag", func() {
					updatedEndpoint := endpoint1
					updatedEndpoint.ModificationTag = olderTag

					actualLRP := getActualLRP(key, updatedEndpoint)
					messagesToEmit = table.AddEndpointFromActual(actualLRP)
					Expect(messagesToEmit).To(BeZero())
				})

				It("emits nothing when updating an endpoint with a newer tag", func() {
					updatedEndpoint := endpoint1
					updatedEndpoint.ModificationTag = newerTag

					actualLRP := getActualLRP(key, updatedEndpoint)
					messagesToEmit = table.AddEndpointFromActual(actualLRP)
					Expect(messagesToEmit).To(BeZero())
				})

				It("emits registrations when adding an endpoint", func() {
					actualLRP := getActualLRP(key, endpoint3)
					messagesToEmit = table.AddEndpointFromActual(actualLRP)

					expected := routing_table.MessagesToEmit{
						RegistrationMessages: []routing_table.RegistryMessage{
							routing_table.RegistryMessageFor(endpoint3, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
						},
					}
					Expect(messagesToEmit).To(MatchMessagesToEmit(expected))
				})

				Context("when an evacuating endpoint is added for an instance that already exists", func() {
					It("emits nothing", func() {
						actualLRP := getActualLRP(key, evacuating1)
						messagesToEmit = table.AddEndpointFromActual(actualLRP)
						Expect(messagesToEmit).To(BeZero())
					})
				})

				Context("when an instance endpoint is updated for an evacuating that already exists", func() {
					BeforeEach(func() {
						table.AddEndpoint(key, evacuating1)
					})

					It("emits nothing", func() {
						actualLRP := getActualLRP(key, endpoint1)
						messagesToEmit = table.AddEndpointFromActual(actualLRP)
						Expect(messagesToEmit).To(BeZero())
					})
				})

				Context("when the endpoint does not already exist", func() {
					It("emits registrations", func() {
						actualLRP := getActualLRP(key, endpoint3)
						messagesToEmit = table.AddEndpointFromActual(actualLRP)

						expected := routing_table.MessagesToEmit{
							RegistrationMessages: []routing_table.RegistryMessage{
								routing_table.RegistryMessageFor(endpoint3, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
							},
						}
						Expect(messagesToEmit).To(MatchMessagesToEmit(expected))
					})
				})
			})

			Context("when removing endpoints", func() {
				It("emits unregistrations with the same tag", func() {
					messagesToEmit = table.RemoveEndpoint(key, endpoint2)

					expected := routing_table.MessagesToEmit{
						UnregistrationMessages: []routing_table.RegistryMessage{
							routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
						},
					}
					Expect(messagesToEmit).To(MatchMessagesToEmit(expected))
				})

				It("emits unregistrations when the tag is newer", func() {
					newerEndpoint := endpoint2
					newerEndpoint.ModificationTag = newerTag
					messagesToEmit = table.RemoveEndpoint(key, newerEndpoint)

					expected := routing_table.MessagesToEmit{
						UnregistrationMessages: []routing_table.RegistryMessage{
							routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
						},
					}
					Expect(messagesToEmit).To(MatchMessagesToEmit(expected))
				})

				It("emits nothing when the tag is older", func() {
					olderEndpoint := endpoint2
					olderEndpoint.ModificationTag = olderTag
					messagesToEmit = table.RemoveEndpoint(key, olderEndpoint)
					Expect(messagesToEmit).To(BeZero())
				})

				Context("when an instance endpoint is removed for an instance that already exists", func() {
					BeforeEach(func() {
						actualLRP := getActualLRP(key, evacuating1)
						messagesToEmit = table.AddEndpointFromActual(actualLRP)
					})

					It("emits nothing", func() {
						messagesToEmit = table.RemoveEndpoint(key, endpoint1)
						Expect(messagesToEmit).To(BeZero())
					})
				})

				Context("when an evacuating endpoint is removed instance that already exists", func() {
					BeforeEach(func() {
						actualLRP := getActualLRP(key, evacuating1)
						messagesToEmit = table.AddEndpointFromActual(actualLRP)
					})

					It("emits nothing", func() {
						actualLRP := getActualLRP(key, endpoint1)
						messagesToEmit = table.AddEndpointFromActual(actualLRP)
						Expect(messagesToEmit).To(BeZero())
					})
				})
			})
		})

		Context("when there are only routes in the table", func() {
			BeforeEach(func() {
				desiredLRP := getDesiredLRP(key, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid})
				table.SetRoutesFromDesired(desiredLRP)
			})

			Context("When setting routes", func() {
				It("emits nothing", func() {
					desiredLRP := getDesiredLRP(key, routing_table.Routes{Hostnames: []string{hostname1, hostname3}, LogGuid: logGuid})
					messagesToEmit = table.SetRoutesFromDesired(desiredLRP)
					Expect(messagesToEmit).To(BeZero())
				})
			})

			Context("when removing routes", func() {
				It("emits nothing", func() {
					messagesToEmit = table.RemoveRoutes(key, currentTag)
					Expect(messagesToEmit).To(BeZero())
				})
			})

			Context("when adding/updating endpoints", func() {
				It("emits registrations", func() {
					actualLRP := getActualLRP(key, endpoint1)
					messagesToEmit = table.AddEndpointFromActual(actualLRP)

					expected := routing_table.MessagesToEmit{
						RegistrationMessages: []routing_table.RegistryMessage{
							routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
						},
					}
					Expect(messagesToEmit).To(MatchMessagesToEmit(expected))
				})
			})
		})

		Context("when there are only endpoints in the table", func() {
			BeforeEach(func() {
				actualLRP := getActualLRP(key, endpoint1)
				messagesToEmit = table.AddEndpointFromActual(actualLRP)

				actualLRP2 := getActualLRP(key, endpoint2)
				messagesToEmit = table.AddEndpointFromActual(actualLRP2)
			})

			Context("When setting routes", func() {
				It("emits registrations", func() {
					desiredLRP := getDesiredLRP(key, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid, ModificationTag: currentTag})
					messagesToEmit = table.SetRoutesFromDesired(desiredLRP)

					expected := routing_table.MessagesToEmit{
						RegistrationMessages: []routing_table.RegistryMessage{
							routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
							routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
						},
					}
					Expect(messagesToEmit).To(MatchMessagesToEmit(expected))
				})
			})

			Context("when removing routes", func() {
				It("emits nothing", func() {
					messagesToEmit = table.RemoveRoutes(key, currentTag)
					Expect(messagesToEmit).To(BeZero())
				})
			})

			Context("when adding/updating endpoints", func() {
				It("emits nothing", func() {
					actualLRP := getActualLRP(key, endpoint2)
					messagesToEmit = table.AddEndpointFromActual(actualLRP)

					Expect(messagesToEmit).To(BeZero())
				})
			})

			Context("when removing endpoints", func() {
				It("emits nothing", func() {
					messagesToEmit = table.RemoveEndpoint(key, endpoint1)
					Expect(messagesToEmit).To(BeZero())
				})
			})
		})
	})

	Describe("MessagesToEmit", func() {
		Context("when the table is empty", func() {
			It("should be empty", func() {
				messagesToEmit = table.MessagesToEmit()
				Expect(messagesToEmit).To(BeZero())
			})
		})

		Context("when the table has routes but no endpoints", func() {
			BeforeEach(func() {
				desiredLRP := getDesiredLRP(key, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid})
				table.SetRoutesFromDesired(desiredLRP)
			})

			It("should be empty", func() {
				messagesToEmit = table.MessagesToEmit()
				Expect(messagesToEmit).To(BeZero())
			})
		})

		Context("when the table has endpoints but no routes", func() {
			BeforeEach(func() {
				actualLRP := getActualLRP(key, endpoint1)
				messagesToEmit = table.AddEndpointFromActual(actualLRP)

				actualLRP2 := getActualLRP(key, endpoint2)
				messagesToEmit = table.AddEndpointFromActual(actualLRP2)
			})

			It("should be empty", func() {
				messagesToEmit = table.MessagesToEmit()
				Expect(messagesToEmit).To(BeZero())
			})
		})

		Context("when the table has routes and endpoints", func() {
			BeforeEach(func() {
				desiredLRP := getDesiredLRP(key, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid})
				table.SetRoutesFromDesired(desiredLRP)
				actualLRP := getActualLRP(key, endpoint1)
				messagesToEmit = table.AddEndpointFromActual(actualLRP)

				actualLRP2 := getActualLRP(key, endpoint2)
				messagesToEmit = table.AddEndpointFromActual(actualLRP2)
			})

			It("emits the registrations", func() {
				messagesToEmit = table.MessagesToEmit()

				expected := routing_table.MessagesToEmit{
					RegistrationMessages: []routing_table.RegistryMessage{
						routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
						routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
					},
				}
				Expect(messagesToEmit).To(MatchMessagesToEmit(expected))
			})
		})
	})

	Describe("RouteCount", func() {
		It("returns 0 on a new routing table", func() {
			Expect(table.RouteCount()).To(Equal(0))
		})

		It("returns 1 after adding a route to a single process", func() {
			desiredLRP := getDesiredLRP(routing_table.RoutingKey{ProcessGuid: "fake-process-guid"}, routing_table.Routes{Hostnames: []string{"fake-route-url"}, LogGuid: logGuid})
			table.SetRoutesFromDesired(desiredLRP)

			Expect(table.RouteCount()).To(Equal(1))
		})

		It("returns 2 after associating 2 urls with a single process", func() {
			desiredLRP := getDesiredLRP(routing_table.RoutingKey{ProcessGuid: "fake-process-guid"}, routing_table.Routes{Hostnames: []string{"fake-route-url-1", "fake-route-url-2"}, LogGuid: logGuid})
			table.SetRoutesFromDesired(desiredLRP)

			Expect(table.RouteCount()).To(Equal(2))
		})

		It("returns 4 after associating 2 urls with two processes", func() {
			desiredLRP := getDesiredLRP(routing_table.RoutingKey{ProcessGuid: "fake-process-guid-a"}, routing_table.Routes{Hostnames: []string{"fake-route-url-a-1", "fake-route-url-a-2"}, LogGuid: logGuid})
			table.SetRoutesFromDesired(desiredLRP)
			desiredLRP = getDesiredLRP(routing_table.RoutingKey{ProcessGuid: "fake-process-guid-b"}, routing_table.Routes{Hostnames: []string{"fake-route-url-b-1", "fake-route-url-b-2"}, LogGuid: logGuid})
			table.SetRoutesFromDesired(desiredLRP)

			Expect(table.RouteCount()).To(Equal(4))
		})
	})

	Describe("UpdateRoutesFromDesired", func() {
		var (
			originalDesiredLRP receptor.DesiredLRPResponse
			changedDesiredLRP  receptor.DesiredLRPResponse

			expectedAdditionalCFRoute                              cfroutes.CFRoute
			expectedCFRoute                                        cfroutes.CFRoute
			expectedChangedCFRoute                                 cfroutes.CFRoute
			expectedContainerPort, expectedAdditionalContainerPort uint16
		)

		BeforeEach(func() {
			expectedContainerPort = uint16(8080)
			expectedAdditionalContainerPort = uint16(9090)
			expectedProcessGuid := "process-guid"

			expectedRoutes := []string{hostname1}
			expectedCFRoute = cfroutes.CFRoute{Hostnames: expectedRoutes, Port: expectedContainerPort}

			expectedAdditionalRoutes := []string{"additional-1", "additional-2"}
			expectedAdditionalCFRoute = cfroutes.CFRoute{Hostnames: expectedAdditionalRoutes, Port: expectedAdditionalContainerPort}

			changedRoutes := []string{hostname1, hostname2}
			expectedChangedCFRoute = cfroutes.CFRoute{Hostnames: changedRoutes, Port: expectedContainerPort}

			key = routing_table.RoutingKey{
				ProcessGuid:   expectedProcessGuid,
				ContainerPort: expectedContainerPort,
			}

			originalDesiredLRP = receptor.DesiredLRPResponse{
				Action: &models.RunAction{
					Path: "ls",
				},
				Domain:      "tests",
				ProcessGuid: expectedProcessGuid,
				LogGuid:     logGuid,
				Ports:       []uint16{expectedContainerPort},
				Routes:      cfroutes.CFRoutes{expectedCFRoute}.RoutingInfo(),
			}
			table.SetRoutesFromDesired(originalDesiredLRP)
			actualLRP := getActualLRP(key, endpoint1)
			messagesToEmit = table.AddEndpointFromActual(actualLRP)

			actualLRP2 := getActualLRP(key, endpoint2)
			messagesToEmit = table.AddEndpointFromActual(actualLRP2)

			changedDesiredLRP = receptor.DesiredLRPResponse{
				Action: &models.RunAction{
					Path: "ls",
				},
				Domain:          "tests",
				ProcessGuid:     expectedProcessGuid,
				LogGuid:         logGuid,
				Ports:           []uint16{expectedContainerPort},
				Routes:          cfroutes.CFRoutes{expectedChangedCFRoute}.RoutingInfo(),
				ModificationTag: receptor.ModificationTag{Epoch: "abcd", Index: 1},
			}

		})

		It("returns the registration messages for new and existing routes", func() {
			expected := routing_table.MessagesToEmit{
				RegistrationMessages: []routing_table.RegistryMessage{
					routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
					routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
				},
			}
			Expect(table.UpdateRoutesFromDesired(originalDesiredLRP, changedDesiredLRP)).To(MatchMessagesToEmit(expected))
		})

		Context("when there are no changes to route", func() {
			BeforeEach(func() {
				changedDesiredLRP.Routes = cfroutes.CFRoutes{expectedCFRoute}.RoutingInfo()
			})

			It("returns no registration messages", func() {
				Expect(table.UpdateRoutesFromDesired(originalDesiredLRP, changedDesiredLRP)).To(BeZero())
			})
		})

		Context("when CF routes are added without an associated container port", func() {
			BeforeEach(func() {
				changedDesiredLRP.Routes = cfroutes.CFRoutes{expectedChangedCFRoute, expectedAdditionalCFRoute}.RoutingInfo()
			})

			It("returns the registration messages only for routes with a port on the desired lrp", func() {
				expected := routing_table.MessagesToEmit{
					RegistrationMessages: []routing_table.RegistryMessage{
						routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
						routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname1, hostname2}, LogGuid: logGuid}),
					},
				}
				Expect(table.UpdateRoutesFromDesired(originalDesiredLRP, changedDesiredLRP)).To(MatchMessagesToEmit(expected))
			})
		})

		Context("when CF routes are removed", func() {
			BeforeEach(func() {
				changedDesiredLRP.Routes = cfroutes.CFRoutes{}.RoutingInfo()
			})

			It("returns the unregistration messages for missing hosts", func() {
				expected := routing_table.MessagesToEmit{
					UnregistrationMessages: []routing_table.RegistryMessage{
						routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname1}, LogGuid: logGuid}),
						routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname1}, LogGuid: logGuid}),
					},
				}
				Expect(table.UpdateRoutesFromDesired(originalDesiredLRP, changedDesiredLRP)).To(MatchMessagesToEmit(expected))
			})
		})

		Context("when container ports are removed", func() {
			BeforeEach(func() {
				changedDesiredLRP.Ports = []uint16{}
				changedDesiredLRP.Routes = cfroutes.CFRoutes{expectedCFRoute}.RoutingInfo()
			})

			It("returns the unregistration messages for missing ports", func() {
				expected := routing_table.MessagesToEmit{
					UnregistrationMessages: []routing_table.RegistryMessage{
						routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname1}, LogGuid: logGuid}),
						routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname1}, LogGuid: logGuid}),
					},
				}
				Expect(table.UpdateRoutesFromDesired(originalDesiredLRP, changedDesiredLRP)).To(MatchMessagesToEmit(expected))
			})
		})
	})
})
