package watcher_test

import (
	"errors"
	"os"

	"github.com/cloudfoundry-incubator/receptor"
	"github.com/cloudfoundry-incubator/receptor/fake_receptor"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/tedsuo/ifrit"

	"github.com/cloudfoundry-incubator/route-emitter/cfroutes"
	"github.com/cloudfoundry-incubator/route-emitter/nats_emitter/fake_nats_emitter"
	"github.com/cloudfoundry-incubator/route-emitter/routing_table"
	"github.com/cloudfoundry-incubator/route-emitter/routing_table/fake_routing_table"
	"github.com/cloudfoundry-incubator/route-emitter/syncer"
	. "github.com/cloudfoundry-incubator/route-emitter/watcher"
	fake_metrics_sender "github.com/cloudfoundry/dropsonde/metric_sender/fake"
	"github.com/cloudfoundry/dropsonde/metrics"
)

const logGuid = "some-log-guid"

var _ = Describe("Watcher", func() {
	const (
		expectedProcessGuid             = "process-guid"
		expectedInstanceGuid            = "instance-guid"
		expectedHost                    = "1.1.1.1"
		expectedExternalPort            = 11000
		expectedAdditionalExternalPort  = 22000
		expectedContainerPort           = 11
		expectedAdditionalContainerPort = 22
	)

	var (
		receptorClient *fake_receptor.FakeClient
		table          *fake_routing_table.FakeRoutingTable
		emitter        *fake_nats_emitter.FakeNATSEmitter
		syncEvents     syncer.SyncEvents

		watcher *Watcher
		process ifrit.Process

		expectedRoutes     []string
		expectedRoutingKey routing_table.RoutingKey
		expectedCFRoute    cfroutes.CFRoute

		expectedAdditionalRoutes     []string
		expectedAdditionalRoutingKey routing_table.RoutingKey
		expectedAdditionalCFRoute    cfroutes.CFRoute

		dummyMessagesToEmit routing_table.MessagesToEmit
		fakeMetricSender    *fake_metrics_sender.FakeMetricSender

		logger *lagertest.TestLogger
	)

	BeforeEach(func() {
		receptorClient = new(fake_receptor.FakeClient)
		table = &fake_routing_table.FakeRoutingTable{}
		emitter = &fake_nats_emitter.FakeNATSEmitter{}
		syncEvents = syncer.SyncEvents{
			Begin: make(chan syncer.SyncBegin),
			End:   make(chan syncer.SyncEnd),
			Emit:  make(chan struct{}),
		}
		logger = lagertest.NewTestLogger("test")

		dummyEndpoint := routing_table.Endpoint{InstanceGuid: expectedInstanceGuid, Host: expectedHost, Port: expectedContainerPort}
		dummyMessage := routing_table.RegistryMessageFor(dummyEndpoint, routing_table.Routes{Hostnames: []string{"foo.com", "bar.com"}, LogGuid: logGuid})
		dummyMessagesToEmit = routing_table.MessagesToEmit{
			RegistrationMessages: []routing_table.RegistryMessage{dummyMessage},
		}

		watcher = NewWatcher(receptorClient, table, emitter, syncEvents, logger)

		expectedRoutes = []string{"route-1", "route-2"}
		expectedCFRoute = cfroutes.CFRoute{Hostnames: expectedRoutes, Port: expectedContainerPort}
		expectedRoutingKey = routing_table.RoutingKey{
			ProcessGuid:   expectedProcessGuid,
			ContainerPort: expectedContainerPort,
		}

		expectedAdditionalRoutes = []string{"additional-1", "additional-2"}
		expectedAdditionalCFRoute = cfroutes.CFRoute{Hostnames: expectedAdditionalRoutes, Port: expectedAdditionalContainerPort}
		expectedAdditionalRoutingKey = routing_table.RoutingKey{
			ProcessGuid:   expectedProcessGuid,
			ContainerPort: expectedAdditionalContainerPort,
		}
		fakeMetricSender = fake_metrics_sender.NewFakeMetricSender()
		metrics.Initialize(fakeMetricSender)
	})

	JustBeforeEach(func() {
		process = ifrit.Invoke(watcher)
	})

	AfterEach(func() {
		process.Signal(os.Interrupt)
		Eventually(process.Wait()).Should(Receive())
	})

	Describe("Desired LRP changes", func() {
		Context("when a create event occurs", func() {
			var desiredLRP receptor.DesiredLRPResponse

			BeforeEach(func() {
				table.SetRoutesReturns(dummyMessagesToEmit)

				eventSource := new(fake_receptor.FakeEventSource)
				receptorClient.SubscribeToEventsReturns(eventSource, nil)

				desiredLRP = receptor.DesiredLRPResponse{
					Action: &models.RunAction{
						Path: "ls",
					},
					Domain:      "tests",
					ProcessGuid: expectedProcessGuid,
					Ports:       []uint16{expectedContainerPort},
					Routes:      cfroutes.CFRoutes{expectedCFRoute}.RoutingInfo(),
					LogGuid:     logGuid,
				}

				var nextErr error
				eventSource.CloseStub = func() error {
					nextErr = errors.New("closed")
					return nil
				}

				eventSource.NextStub = func() (receptor.Event, error) {
					if eventSource.NextCallCount() == 1 {
						return receptor.NewDesiredLRPCreatedEvent(desiredLRP), nil
					} else {
						return nil, nextErr
					}
				}
			})

			It("should set the routes on the table", func() {
				Eventually(table.SetRoutesCallCount).Should(Equal(1))

				key, routes := table.SetRoutesArgsForCall(0)
				Ω(key).Should(Equal(expectedRoutingKey))
				Ω(routes).Should(Equal(routing_table.Routes{Hostnames: expectedRoutes, LogGuid: logGuid}))
			})

			It("sends a 'routes registered' metric", func() {
				Eventually(func() uint64 {
					return fakeMetricSender.GetCounter("RoutesRegistered")
				}).Should(BeEquivalentTo(2))
			})

			It("sends a 'routes unregistered' metric", func() {
				Eventually(func() uint64 {
					return fakeMetricSender.GetCounter("RoutesUnRegistered")
				}).Should(BeEquivalentTo(0))
			})

			It("should emit whatever the table tells it to emit", func() {
				Eventually(emitter.EmitCallCount).Should(Equal(1))
				messagesToEmit := emitter.EmitArgsForCall(0)
				Ω(messagesToEmit).Should(Equal(dummyMessagesToEmit))
			})

			Context("when there are multiple CF routes", func() {
				BeforeEach(func() {
					desiredLRP.Ports = []uint16{expectedContainerPort, expectedAdditionalContainerPort}
					desiredLRP.Routes = cfroutes.CFRoutes{expectedCFRoute, expectedAdditionalCFRoute}.RoutingInfo()
				})

				It("registers all of the routes on the table", func() {
					Eventually(table.SetRoutesCallCount).Should(Equal(2))

					key, routes := table.SetRoutesArgsForCall(0)
					Ω(key).Should(Equal(expectedRoutingKey))
					Ω(routes).Should(Equal(routing_table.Routes{Hostnames: expectedRoutes, LogGuid: logGuid}))

					key, routes = table.SetRoutesArgsForCall(1)
					Ω(key).Should(Equal(expectedAdditionalRoutingKey))
					Ω(routes).Should(Equal(routing_table.Routes{Hostnames: expectedAdditionalRoutes, LogGuid: logGuid}))
				})

				It("emits whatever the table tells it to emit", func() {
					Eventually(emitter.EmitCallCount).Should(Equal(2))

					messagesToEmit := emitter.EmitArgsForCall(0)
					Ω(messagesToEmit).Should(Equal(dummyMessagesToEmit))

					messagesToEmit = emitter.EmitArgsForCall(1)
					Ω(messagesToEmit).Should(Equal(dummyMessagesToEmit))
				})
			})
		})

		Context("when a change event occurs", func() {
			var originalDesiredLRP receptor.DesiredLRPResponse
			var changedDesiredLRP receptor.DesiredLRPResponse

			BeforeEach(func() {
				table.SetRoutesReturns(dummyMessagesToEmit)

				eventSource := new(fake_receptor.FakeEventSource)
				receptorClient.SubscribeToEventsReturns(eventSource, nil)

				originalDesiredLRP = receptor.DesiredLRPResponse{
					Action: &models.RunAction{
						Path: "ls",
					},
					Domain:      "tests",
					ProcessGuid: expectedProcessGuid,
					LogGuid:     logGuid,
					Ports:       []uint16{expectedContainerPort},
				}
				changedDesiredLRP = receptor.DesiredLRPResponse{
					Action: &models.RunAction{
						Path: "ls",
					},
					Domain:          "tests",
					ProcessGuid:     expectedProcessGuid,
					LogGuid:         logGuid,
					Ports:           []uint16{expectedContainerPort},
					Routes:          cfroutes.CFRoutes{{Hostnames: expectedRoutes, Port: expectedContainerPort}}.RoutingInfo(),
					ModificationTag: receptor.ModificationTag{Epoch: "abcd", Index: 1},
				}

				var nextErr error
				eventSource.CloseStub = func() error {
					nextErr = errors.New("closed")
					return nil
				}

				eventSource.NextStub = func() (receptor.Event, error) {
					if eventSource.NextCallCount() == 1 {
						return receptor.NewDesiredLRPChangedEvent(
							originalDesiredLRP,
							changedDesiredLRP,
						), nil
					} else {
						return nil, nextErr
					}
				}
			})

			It("should set the routes on the table", func() {
				Eventually(table.SetRoutesCallCount).Should(Equal(1))
				key, routes := table.SetRoutesArgsForCall(0)
				Ω(key).Should(Equal(expectedRoutingKey))
				Ω(routes).Should(Equal(routing_table.Routes{Hostnames: expectedRoutes, LogGuid: logGuid}))
			})

			It("sends a 'routes registered' metric", func() {
				Eventually(func() uint64 {
					return fakeMetricSender.GetCounter("RoutesRegistered")
				}).Should(BeEquivalentTo(2))
			})

			It("sends a 'routes unregistered' metric", func() {
				Eventually(func() uint64 {
					return fakeMetricSender.GetCounter("RoutesUnRegistered")
				}).Should(BeEquivalentTo(0))
			})

			It("should emit whatever the table tells it to emit", func() {
				Eventually(emitter.EmitCallCount).Should(Equal(1))
				messagesToEmit := emitter.EmitArgsForCall(0)
				Ω(messagesToEmit).Should(Equal(dummyMessagesToEmit))
			})

			Context("when CF routes are added without an associated container port", func() {
				BeforeEach(func() {
					changedDesiredLRP.Ports = []uint16{expectedContainerPort}
					changedDesiredLRP.Routes = cfroutes.CFRoutes{expectedCFRoute, expectedAdditionalCFRoute}.RoutingInfo()
				})

				It("registers all of the routes associated with a port on the table", func() {
					Eventually(table.SetRoutesCallCount).Should(Equal(1))

					key, routes := table.SetRoutesArgsForCall(0)
					Ω(key).Should(Equal(expectedRoutingKey))
					Ω(routes).Should(Equal(routing_table.Routes{Hostnames: expectedRoutes, LogGuid: logGuid}))
				})

				It("emits whatever the table tells it to emit", func() {
					Eventually(emitter.EmitCallCount).Should(Equal(1))

					messagesToEmit := emitter.EmitArgsForCall(0)
					Ω(messagesToEmit).Should(Equal(dummyMessagesToEmit))
				})
			})

			Context("when CF routes and container ports are added", func() {
				BeforeEach(func() {
					changedDesiredLRP.Ports = []uint16{expectedContainerPort, expectedAdditionalContainerPort}
					changedDesiredLRP.Routes = cfroutes.CFRoutes{expectedCFRoute, expectedAdditionalCFRoute}.RoutingInfo()
				})

				It("registers all of the routes on the table", func() {
					Eventually(table.SetRoutesCallCount).Should(Equal(2))

					key, routes := table.SetRoutesArgsForCall(0)
					Ω(key).Should(Equal(expectedRoutingKey))
					Ω(routes).Should(Equal(routing_table.Routes{Hostnames: expectedRoutes, LogGuid: logGuid}))

					key, routes = table.SetRoutesArgsForCall(1)
					Ω(key).Should(Equal(expectedAdditionalRoutingKey))
					Ω(routes).Should(Equal(routing_table.Routes{Hostnames: expectedAdditionalRoutes, LogGuid: logGuid}))
				})

				It("emits whatever the table tells it to emit", func() {
					Eventually(emitter.EmitCallCount).Should(Equal(2))

					messagesToEmit := emitter.EmitArgsForCall(0)
					Ω(messagesToEmit).Should(Equal(dummyMessagesToEmit))

					messagesToEmit = emitter.EmitArgsForCall(1)
					Ω(messagesToEmit).Should(Equal(dummyMessagesToEmit))
				})
			})

			Context("when CF routes are removed", func() {
				BeforeEach(func() {
					changedDesiredLRP.Ports = []uint16{expectedContainerPort}
					changedDesiredLRP.Routes = cfroutes.CFRoutes{}.RoutingInfo()

					table.SetRoutesReturns(routing_table.MessagesToEmit{})
					table.RemoveRoutesReturns(dummyMessagesToEmit)
				})

				It("deletes the routes for the missng key", func() {
					Eventually(table.RemoveRoutesCallCount).Should(Equal(1))

					key, modTag := table.RemoveRoutesArgsForCall(0)
					Ω(key).Should(Equal(expectedRoutingKey))
					Ω(modTag).Should(Equal(changedDesiredLRP.ModificationTag))
				})

				It("emits whatever the table tells it to emit", func() {
					Eventually(emitter.EmitCallCount).Should(Equal(1))

					messagesToEmit := emitter.EmitArgsForCall(0)
					Ω(messagesToEmit).Should(Equal(dummyMessagesToEmit))
				})
			})

			Context("when container ports are removed", func() {
				BeforeEach(func() {
					changedDesiredLRP.Ports = []uint16{}
					changedDesiredLRP.Routes = cfroutes.CFRoutes{expectedCFRoute}.RoutingInfo()

					table.SetRoutesReturns(routing_table.MessagesToEmit{})
					table.RemoveRoutesReturns(dummyMessagesToEmit)
				})

				It("deletes the routes for the missng key", func() {
					Eventually(table.RemoveRoutesCallCount).Should(Equal(1))

					key, modTag := table.RemoveRoutesArgsForCall(0)
					Ω(key).Should(Equal(expectedRoutingKey))
					Ω(modTag).Should(Equal(changedDesiredLRP.ModificationTag))
				})

				It("emits whatever the table tells it to emit", func() {
					Eventually(emitter.EmitCallCount).Should(Equal(1))

					messagesToEmit := emitter.EmitArgsForCall(0)
					Ω(messagesToEmit).Should(Equal(dummyMessagesToEmit))
				})
			})
		})

		Context("when a delete event occurs", func() {
			var desiredLRP receptor.DesiredLRPResponse

			BeforeEach(func() {
				table.RemoveRoutesReturns(dummyMessagesToEmit)

				eventSource := new(fake_receptor.FakeEventSource)
				receptorClient.SubscribeToEventsReturns(eventSource, nil)

				desiredLRP = receptor.DesiredLRPResponse{
					Action: &models.RunAction{
						Path: "ls",
					},
					Domain:          "tests",
					ProcessGuid:     expectedProcessGuid,
					Ports:           []uint16{expectedContainerPort},
					Routes:          cfroutes.CFRoutes{expectedCFRoute}.RoutingInfo(),
					LogGuid:         logGuid,
					ModificationTag: receptor.ModificationTag{Epoch: "defg", Index: 2},
				}

				var nextErr error
				eventSource.CloseStub = func() error {
					nextErr = errors.New("closed")
					return nil
				}

				eventSource.NextStub = func() (receptor.Event, error) {
					if eventSource.NextCallCount() == 1 {
						return receptor.NewDesiredLRPRemovedEvent(desiredLRP), nil
					} else {
						return nil, nextErr
					}
				}
			})

			It("should remove the routes from the table", func() {
				Eventually(table.RemoveRoutesCallCount).Should(Equal(1))
				key, modTag := table.RemoveRoutesArgsForCall(0)
				Ω(key).Should(Equal(expectedRoutingKey))
				Ω(modTag).Should(Equal(desiredLRP.ModificationTag))
			})

			It("should emit whatever the table tells it to emit", func() {
				Eventually(emitter.EmitCallCount).Should(Equal(1))

				messagesToEmit := emitter.EmitArgsForCall(0)
				Ω(messagesToEmit).Should(Equal(dummyMessagesToEmit))
			})

			Context("when there are multiple CF routes", func() {
				BeforeEach(func() {
					desiredLRP.Ports = []uint16{expectedContainerPort, expectedAdditionalContainerPort}
					desiredLRP.Routes = cfroutes.CFRoutes{expectedCFRoute, expectedAdditionalCFRoute}.RoutingInfo()
				})

				It("should remove the routes from the table", func() {
					Eventually(table.RemoveRoutesCallCount).Should(Equal(2))

					key, modTag := table.RemoveRoutesArgsForCall(0)
					Ω(key).Should(Equal(expectedRoutingKey))
					Ω(modTag).Should(Equal(desiredLRP.ModificationTag))

					key, modTag = table.RemoveRoutesArgsForCall(1)
					Ω(key).Should(Equal(expectedAdditionalRoutingKey))

					key, modTag = table.RemoveRoutesArgsForCall(0)
					Ω(key).Should(Equal(expectedRoutingKey))
					Ω(modTag).Should(Equal(desiredLRP.ModificationTag))
				})

				It("emits whatever the table tells it to emit", func() {
					Eventually(emitter.EmitCallCount).Should(Equal(2))

					messagesToEmit := emitter.EmitArgsForCall(0)
					Ω(messagesToEmit).Should(Equal(dummyMessagesToEmit))

					messagesToEmit = emitter.EmitArgsForCall(1)
					Ω(messagesToEmit).Should(Equal(dummyMessagesToEmit))
				})
			})
		})
	})

	Describe("Actual LRP changes", func() {
		Context("when a create event occurs", func() {
			var actualLRP receptor.ActualLRPResponse

			Context("when the resulting LRP is in the RUNNING state", func() {
				BeforeEach(func() {
					table.AddEndpointReturns(dummyMessagesToEmit)

					eventSource := new(fake_receptor.FakeEventSource)
					receptorClient.SubscribeToEventsReturns(eventSource, nil)

					actualLRP = receptor.ActualLRPResponse{
						ProcessGuid:  expectedProcessGuid,
						Index:        1,
						Domain:       "domain",
						InstanceGuid: expectedInstanceGuid,
						CellID:       "cell-id",
						Address:      expectedHost,
						Ports: []receptor.PortMapping{
							{ContainerPort: expectedContainerPort, HostPort: expectedExternalPort},
							{ContainerPort: expectedAdditionalContainerPort, HostPort: expectedExternalPort},
						},
						State: receptor.ActualLRPStateRunning,
					}

					var nextErr error
					eventSource.CloseStub = func() error {
						nextErr = errors.New("closed")
						return nil
					}

					eventSource.NextStub = func() (receptor.Event, error) {
						if eventSource.NextCallCount() == 1 {
							return receptor.NewActualLRPCreatedEvent(actualLRP), nil
						} else {
							return nil, nextErr
						}
					}
				})

				It("should add/update the endpoints on the table", func() {
					Eventually(table.AddEndpointCallCount).Should(Equal(2))

					keys := routing_table.RoutingKeysFromActual(actualLRP)
					endpoints, err := routing_table.EndpointsFromActual(actualLRP)
					Ω(err).ShouldNot(HaveOccurred())

					key, endpoint := table.AddEndpointArgsForCall(0)
					Ω(keys).Should(ContainElement(key))
					Ω(endpoint).Should(Equal(endpoints[key.ContainerPort]))

					key, endpoint = table.AddEndpointArgsForCall(1)
					Ω(keys).Should(ContainElement(key))
					Ω(endpoint).Should(Equal(endpoints[key.ContainerPort]))
				})

				It("should emit whatever the table tells it to emit", func() {
					Eventually(emitter.EmitCallCount).Should(Equal(2))

					messagesToEmit := emitter.EmitArgsForCall(0)
					Ω(messagesToEmit).Should(Equal(dummyMessagesToEmit))
				})

				It("sends a 'routes registered' metric", func() {
					Eventually(func() uint64 {
						return fakeMetricSender.GetCounter("RoutesRegistered")
					}).Should(BeEquivalentTo(4))
				})

				It("sends a 'routes unregistered' metric", func() {
					Eventually(func() uint64 {
						return fakeMetricSender.GetCounter("RoutesUnRegistered")
					}).Should(BeEquivalentTo(0))
				})
			})

			Context("when the resulting LRP is not in the RUNNING state", func() {
				BeforeEach(func() {
					eventSource := new(fake_receptor.FakeEventSource)
					receptorClient.SubscribeToEventsReturns(eventSource, nil)

					actualLRP := receptor.ActualLRPResponse{
						ProcessGuid:  expectedProcessGuid,
						Index:        1,
						Domain:       "domain",
						InstanceGuid: expectedInstanceGuid,
						CellID:       "cell-id",
						Address:      expectedHost,
						Ports: []receptor.PortMapping{
							{ContainerPort: expectedContainerPort, HostPort: expectedExternalPort},
							{ContainerPort: expectedAdditionalContainerPort, HostPort: expectedExternalPort},
						},
						State: receptor.ActualLRPStateUnclaimed,
					}

					var nextErr error
					eventSource.CloseStub = func() error {
						nextErr = errors.New("closed")
						return nil
					}

					eventSource.NextStub = func() (receptor.Event, error) {
						if eventSource.NextCallCount() == 1 {
							return receptor.NewActualLRPCreatedEvent(actualLRP), nil
						} else {
							return nil, nextErr
						}
					}
				})

				It("doesn't add/update the endpoint on the table", func() {
					Consistently(table.AddEndpointCallCount).Should(Equal(0))
				})

				It("doesn't emit", func() {
					Eventually(emitter.EmitCallCount).Should(Equal(0))
				})
			})
		})

		Context("when a change event occurs", func() {
			Context("when the resulting LRP is in the RUNNING state", func() {
				BeforeEach(func() {
					table.AddEndpointReturns(dummyMessagesToEmit)

					eventSource := new(fake_receptor.FakeEventSource)
					receptorClient.SubscribeToEventsReturns(eventSource, nil)

					beforeActualLRP := receptor.ActualLRPResponse{
						ProcessGuid:  expectedProcessGuid,
						Index:        1,
						Domain:       "domain",
						InstanceGuid: expectedInstanceGuid,
						CellID:       "cell-id",
						State:        receptor.ActualLRPStateClaimed,
					}
					afterActualLRP := receptor.ActualLRPResponse{
						ProcessGuid:  expectedProcessGuid,
						Index:        1,
						Domain:       "domain",
						InstanceGuid: expectedInstanceGuid,
						CellID:       "cell-id",
						Address:      expectedHost,
						Ports: []receptor.PortMapping{
							{ContainerPort: expectedContainerPort, HostPort: expectedExternalPort},
							{ContainerPort: expectedAdditionalContainerPort, HostPort: expectedAdditionalExternalPort},
						},
						State: receptor.ActualLRPStateRunning,
					}

					var nextErr error
					eventSource.CloseStub = func() error {
						nextErr = errors.New("closed")
						return nil
					}

					eventSource.NextStub = func() (receptor.Event, error) {
						if eventSource.NextCallCount() == 1 {
							return receptor.NewActualLRPChangedEvent(beforeActualLRP, afterActualLRP), nil
						} else {
							return nil, nextErr
						}
					}
				})

				It("should add/update the endpoint on the table", func() {
					Eventually(table.AddEndpointCallCount).Should(Equal(2))

					key, endpoint := table.AddEndpointArgsForCall(0)
					Ω(key).Should(Equal(expectedRoutingKey))
					Ω(endpoint).Should(Equal(routing_table.Endpoint{
						InstanceGuid:  expectedInstanceGuid,
						Host:          expectedHost,
						Port:          expectedExternalPort,
						ContainerPort: expectedContainerPort,
					}))

					key, endpoint = table.AddEndpointArgsForCall(1)
					Ω(key).Should(Equal(expectedAdditionalRoutingKey))
					Ω(endpoint).Should(Equal(routing_table.Endpoint{
						InstanceGuid:  expectedInstanceGuid,
						Host:          expectedHost,
						Port:          expectedAdditionalExternalPort,
						ContainerPort: expectedAdditionalContainerPort,
					}))
				})

				It("should emit whatever the table tells it to emit", func() {
					Eventually(emitter.EmitCallCount).Should(Equal(2))

					messagesToEmit := emitter.EmitArgsForCall(0)
					Ω(messagesToEmit).Should(Equal(dummyMessagesToEmit))
				})

				It("sends a 'routes registered' metric", func() {
					Eventually(func() uint64 {
						return fakeMetricSender.GetCounter("RoutesRegistered")
					}).Should(BeEquivalentTo(4))
				})

				It("sends a 'routes unregistered' metric", func() {
					Eventually(func() uint64 {
						return fakeMetricSender.GetCounter("RoutesUnRegistered")
					}).Should(BeEquivalentTo(0))
				})
			})

			Context("when the resulting LRP transitions away form the RUNNING state", func() {
				BeforeEach(func() {
					table.RemoveEndpointReturns(dummyMessagesToEmit)

					eventSource := new(fake_receptor.FakeEventSource)
					receptorClient.SubscribeToEventsReturns(eventSource, nil)

					beforeActualLRP := receptor.ActualLRPResponse{
						ProcessGuid:  expectedProcessGuid,
						Index:        1,
						Domain:       "domain",
						InstanceGuid: expectedInstanceGuid,
						CellID:       "cell-id",
						Address:      expectedHost,
						Ports: []receptor.PortMapping{
							{ContainerPort: expectedContainerPort, HostPort: expectedExternalPort},
							{ContainerPort: expectedAdditionalContainerPort, HostPort: expectedAdditionalExternalPort},
						},
						State: receptor.ActualLRPStateRunning,
					}
					afterActualLRP := receptor.ActualLRPResponse{
						ProcessGuid: expectedProcessGuid,
						Index:       1,
						Domain:      "domain",
						State:       receptor.ActualLRPStateUnclaimed,
					}

					var nextErr error
					eventSource.CloseStub = func() error {
						nextErr = errors.New("closed")
						return nil
					}

					eventSource.NextStub = func() (receptor.Event, error) {
						if eventSource.NextCallCount() == 1 {
							return receptor.NewActualLRPChangedEvent(beforeActualLRP, afterActualLRP), nil
						} else {
							return nil, nextErr
						}
					}
				})

				It("should remove the endpoint from the table", func() {
					Eventually(table.RemoveEndpointCallCount).Should(Equal(2))

					key, endpoint := table.RemoveEndpointArgsForCall(0)
					Ω(key).Should(Equal(expectedRoutingKey))
					Ω(endpoint).Should(Equal(routing_table.Endpoint{
						InstanceGuid:  expectedInstanceGuid,
						Host:          expectedHost,
						Port:          expectedExternalPort,
						ContainerPort: expectedContainerPort,
					}))

					key, endpoint = table.RemoveEndpointArgsForCall(1)
					Ω(key).Should(Equal(expectedAdditionalRoutingKey))
					Ω(endpoint).Should(Equal(routing_table.Endpoint{
						InstanceGuid:  expectedInstanceGuid,
						Host:          expectedHost,
						Port:          expectedAdditionalExternalPort,
						ContainerPort: expectedAdditionalContainerPort,
					}))
				})

				It("should emit whatever the table tells it to emit", func() {
					Eventually(emitter.EmitCallCount).Should(Equal(2))

					messagesToEmit := emitter.EmitArgsForCall(0)
					Ω(messagesToEmit).Should(Equal(dummyMessagesToEmit))
				})
			})

			Context("when the endpoint neither starts nor ends in the RUNNING state", func() {
				BeforeEach(func() {
					eventSource := new(fake_receptor.FakeEventSource)
					receptorClient.SubscribeToEventsReturns(eventSource, nil)

					beforeActualLRP := receptor.ActualLRPResponse{
						ProcessGuid: expectedProcessGuid,
						Index:       1,
						Domain:      "domain",
						State:       receptor.ActualLRPStateUnclaimed,
					}
					afterActualLRP := receptor.ActualLRPResponse{
						ProcessGuid:  expectedProcessGuid,
						Index:        1,
						Domain:       "domain",
						InstanceGuid: expectedInstanceGuid,
						CellID:       "cell-id",
						State:        receptor.ActualLRPStateClaimed,
					}

					var nextErr error
					eventSource.CloseStub = func() error {
						nextErr = errors.New("closed")
						return nil
					}

					eventSource.NextStub = func() (receptor.Event, error) {
						if eventSource.NextCallCount() == 1 {
							return receptor.NewActualLRPChangedEvent(beforeActualLRP, afterActualLRP), nil
						} else {
							return nil, nextErr
						}
					}
				})

				It("should not remove the endpoint", func() {
					Consistently(table.RemoveEndpointCallCount).Should(BeZero())
				})

				It("should not add or update the endpoint", func() {
					Consistently(table.AddEndpointCallCount).Should(BeZero())
				})

				It("should not emit anything", func() {
					Consistently(emitter.EmitCallCount).Should(BeZero())
				})
			})
		})

		Context("when a delete event occurs", func() {
			Context("when the actual is in the RUNNING state", func() {
				BeforeEach(func() {
					table.RemoveEndpointReturns(dummyMessagesToEmit)

					eventSource := new(fake_receptor.FakeEventSource)
					receptorClient.SubscribeToEventsReturns(eventSource, nil)

					actualLRP := receptor.ActualLRPResponse{
						ProcessGuid:  expectedProcessGuid,
						Index:        1,
						Domain:       "domain",
						InstanceGuid: expectedInstanceGuid,
						CellID:       "cell-id",
						Address:      expectedHost,
						Ports: []receptor.PortMapping{
							{ContainerPort: expectedContainerPort, HostPort: expectedExternalPort},
							{ContainerPort: expectedAdditionalContainerPort, HostPort: expectedAdditionalExternalPort},
						},
						State: receptor.ActualLRPStateRunning,
					}

					var nextErr error
					eventSource.CloseStub = func() error {
						nextErr = errors.New("closed")
						return nil
					}

					eventSource.NextStub = func() (receptor.Event, error) {
						if eventSource.NextCallCount() == 1 {
							return receptor.NewActualLRPRemovedEvent(actualLRP), nil
						} else {
							return nil, nextErr
						}
					}
				})

				It("should remove the endpoint from the table", func() {
					Eventually(table.RemoveEndpointCallCount).Should(Equal(2))

					key, endpoint := table.RemoveEndpointArgsForCall(0)
					Ω(key).Should(Equal(expectedRoutingKey))
					Ω(endpoint).Should(Equal(routing_table.Endpoint{
						InstanceGuid:  expectedInstanceGuid,
						Host:          expectedHost,
						Port:          expectedExternalPort,
						ContainerPort: expectedContainerPort,
					}))

					key, endpoint = table.RemoveEndpointArgsForCall(1)
					Ω(key).Should(Equal(expectedAdditionalRoutingKey))
					Ω(endpoint).Should(Equal(routing_table.Endpoint{
						InstanceGuid:  expectedInstanceGuid,
						Host:          expectedHost,
						Port:          expectedAdditionalExternalPort,
						ContainerPort: expectedAdditionalContainerPort,
					}))
				})

				It("should emit whatever the table tells it to emit", func() {
					Eventually(emitter.EmitCallCount).Should(Equal(2))

					messagesToEmit := emitter.EmitArgsForCall(0)
					Ω(messagesToEmit).Should(Equal(dummyMessagesToEmit))

					messagesToEmit = emitter.EmitArgsForCall(1)
					Ω(messagesToEmit).Should(Equal(dummyMessagesToEmit))
				})
			})

			Context("when the actual is not in the RUNNING state", func() {
				BeforeEach(func() {
					eventSource := new(fake_receptor.FakeEventSource)
					receptorClient.SubscribeToEventsReturns(eventSource, nil)

					actualLRP := receptor.ActualLRPResponse{
						ProcessGuid: expectedProcessGuid,
						Index:       1,
						Domain:      "domain",
						State:       receptor.ActualLRPStateCrashed,
					}

					var nextErr error
					eventSource.CloseStub = func() error {
						nextErr = errors.New("closed")
						return nil
					}

					eventSource.NextStub = func() (receptor.Event, error) {
						if eventSource.NextCallCount() == 1 {
							return receptor.NewActualLRPRemovedEvent(actualLRP), nil
						} else {
							return nil, nextErr
						}
					}
				})

				It("doesn't remove the endpoint from the table", func() {
					Consistently(table.RemoveEndpointCallCount).Should(Equal(0))
				})

				It("doesn't emit", func() {
					Consistently(emitter.EmitCallCount).Should(Equal(0))
				})
			})
		})
	})

	Describe("Unrecognized events", func() {
		BeforeEach(func() {
			eventSource := new(fake_receptor.FakeEventSource)
			receptorClient.SubscribeToEventsReturns(eventSource, nil)

			var nextErr error
			eventSource.CloseStub = func() error {
				nextErr = errors.New("closed")
				return nil
			}

			eventSource.NextStub = func() (receptor.Event, error) {
				if eventSource.NextCallCount() == 1 {
					return unrecognizedEvent{}, nil
				} else {
					return nil, nextErr
				}
			}
		})

		It("does not emit any messages", func() {
			Consistently(emitter.EmitCallCount).Should(BeZero())
		})
	})

	Context("when the event source returns an error", func() {
		var subscribeErr, nextErr error

		BeforeEach(func() {
			subscribeErr = errors.New("subscribe-error")
			nextErr = errors.New("next-error")

			eventSource := new(fake_receptor.FakeEventSource)
			receptorClient.SubscribeToEventsStub = func() (receptor.EventSource, error) {
				if receptorClient.SubscribeToEventsCallCount() == 1 {
					return eventSource, nil
				}
				return nil, subscribeErr
			}

			eventSource.NextStub = func() (receptor.Event, error) {
				return nil, nextErr
			}
		})

		It("re-subscribes", func() {
			Eventually(receptorClient.SubscribeToEventsCallCount).Should(Equal(2))
		})

		Context("when re-subscribing fails", func() {
			It("returns an error", func() {
				Eventually(process.Wait()).Should(Receive(Equal(subscribeErr)))
			})
		})
	})

	Describe("interrupting the process", func() {
		BeforeEach(func() {
			eventSource := new(fake_receptor.FakeEventSource)
			receptorClient.SubscribeToEventsReturns(eventSource, nil)
		})

		It("should be possible to SIGINT the route emitter", func() {
			process.Signal(os.Interrupt)
			Eventually(process.Wait()).Should(Receive())
		})
	})

	Describe("Sync Events", func() {
		var nextEvent chan receptor.Event

		BeforeEach(func() {
			eventSource := new(fake_receptor.FakeEventSource)
			receptorClient.SubscribeToEventsReturns(eventSource, nil)
			nextEvent = make(chan receptor.Event)

			var nextErr error
			eventSource.CloseStub = func() error {
				nextErr = errors.New("closed")
				return nil
			}

			eventSource.NextStub = func() (receptor.Event, error) {
				select {
				case e := <-nextEvent:
					return e, nil
				default:
				}

				return nil, nextErr
			}
		})

		Context("Emit", func() {
			JustBeforeEach(func() {
				table.MessagesToEmitReturns(dummyMessagesToEmit)
				table.RouteCountReturns(123)
				syncEvents.Emit <- struct{}{}
			})

			It("emits", func() {
				Eventually(emitter.EmitCallCount).Should(Equal(1))
				Ω(emitter.EmitArgsForCall(0)).Should(Equal(dummyMessagesToEmit))
			})

			It("sends a 'routes total' metric", func() {
				Eventually(func() float64 {
					return fakeMetricSender.GetValue("RoutesTotal").Value
				}, 2).Should(BeEquivalentTo(123))
			})

			It("sends a 'synced routes' metric", func() {
				Eventually(func() uint64 {
					return fakeMetricSender.GetCounter("RoutesSynced")
				}, 2).Should(BeEquivalentTo(2))
			})
		})

		Context("Begin & End events", func() {
			currentTag := receptor.ModificationTag{Epoch: "abc", Index: 1}
			hostname1 := "foo.example.com"
			hostname2 := "bar.example.com"
			endpoint1 := routing_table.Endpoint{InstanceGuid: "ig-1", Host: "1.1.1.1", Port: 11, ContainerPort: 8080, Evacuating: false, ModificationTag: currentTag}
			endpoint2 := routing_table.Endpoint{InstanceGuid: "ig-2", Host: "2.2.2.2", Port: 22, ContainerPort: 8080, Evacuating: false, ModificationTag: currentTag}

			desiredLRP1 := receptor.DesiredLRPResponse{
				Action: &models.RunAction{
					Path: "ls",
				},
				Domain:      "tests",
				ProcessGuid: "pg-1",
				Ports:       []uint16{8080},
				Routes: cfroutes.CFRoutes{
					cfroutes.CFRoute{
						Hostnames: []string{hostname1},
						Port:      8080,
					},
				}.RoutingInfo(),
				LogGuid: "lg1",
			}

			desiredLRP2 := receptor.DesiredLRPResponse{
				Action: &models.RunAction{
					Path: "ls",
				},
				Domain:      "tests",
				ProcessGuid: "pg-2",
				Ports:       []uint16{8080},
				Routes: cfroutes.CFRoutes{
					cfroutes.CFRoute{
						Hostnames: []string{hostname2},
						Port:      8080,
					},
				}.RoutingInfo(),
				LogGuid: "lg2",
			}

			actualLRP1 := receptor.ActualLRPResponse{
				ProcessGuid:  "pg-1",
				Index:        1,
				Domain:       "domain",
				InstanceGuid: endpoint1.InstanceGuid,
				CellID:       "cell-id",
				Address:      endpoint1.Host,
				Ports: []receptor.PortMapping{
					{ContainerPort: endpoint1.ContainerPort, HostPort: endpoint1.Port},
				},
				State: receptor.ActualLRPStateRunning,
			}

			actualLRP2 := receptor.ActualLRPResponse{
				ProcessGuid:  "pg-2",
				Index:        1,
				Domain:       "domain",
				InstanceGuid: endpoint2.InstanceGuid,
				CellID:       "cell-id",
				Address:      endpoint2.Host,
				Ports: []receptor.PortMapping{
					{ContainerPort: endpoint2.ContainerPort, HostPort: endpoint2.Port},
				},
				State: receptor.ActualLRPStateRunning,
			}

			var ack chan struct{}

			sendEvent := func() {
				nextEvent <- receptor.NewActualLRPRemovedEvent(actualLRP1)
			}

			JustBeforeEach(func() {
				ack = make(chan struct{})
				syncEvents.Begin <- syncer.SyncBegin{ack}
				Eventually(ack).Should(BeClosed())
			})

			Context("when sync begins", func() {
				It("caches events", func() {
					sendEvent()
					Consistently(table.RemoveEndpointCallCount).Should(Equal(0))
				})
			})

			Context("when syncing ends", func() {
				var tempTable routing_table.RoutingTable
				var callback func(routing_table.RoutingTable)

				BeforeEach(func() {
					tempTable = &fake_routing_table.FakeRoutingTable{}
					callback = nil
				})

				sendEnd := func() {
					syncEvents.End <- syncer.SyncEnd{
						Table:    tempTable,
						Callback: callback,
					}
				}

				It("swaps the tables", func() {
					sendEnd()

					Eventually(table.SwapCallCount).Should(Equal(1))
					Ω(table.SwapArgsForCall(0)).Should(Equal(tempTable))
				})

				Context("a table with a single routable endpoint", func() {
					BeforeEach(func() {
						tempTable = routing_table.NewTempTable(
							routing_table.RoutesByRoutingKeyFromDesireds([]receptor.DesiredLRPResponse{desiredLRP1, desiredLRP2}),
							routing_table.EndpointsByRoutingKeyFromActuals([]receptor.ActualLRPResponse{actualLRP1, actualLRP2}),
						)

						table := routing_table.NewTable()
						table.Swap(tempTable)

						watcher = NewWatcher(receptorClient, table, emitter, syncEvents, logger)

						tempTable = routing_table.NewTempTable(
							routing_table.RoutesByRoutingKeyFromDesireds([]receptor.DesiredLRPResponse{desiredLRP1, desiredLRP2}),
							routing_table.EndpointsByRoutingKeyFromActuals([]receptor.ActualLRPResponse{actualLRP1, actualLRP2}),
						)
					})

					It("applies the cached events and emits", func() {
						sendEvent()
						sendEnd()

						Eventually(emitter.EmitCallCount).Should(Equal(1))
						Ω(emitter.EmitArgsForCall(0)).Should(Equal(routing_table.MessagesToEmit{
							RegistrationMessages: []routing_table.RegistryMessage{
								routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname2}, LogGuid: "lg2"}),
							},
							UnregistrationMessages: []routing_table.RegistryMessage{
								routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname1}, LogGuid: "lg1"}),
							},
						}))
					})
				})

				Context("when a callback is provided", func() {
					var called chan struct{}

					BeforeEach(func() {
						called = make(chan struct{})
						callback = func(routing_table.RoutingTable) {
							close(called)
						}
					})

					It("calls the callback", func() {
						sendEnd()
						Eventually(called).Should(BeClosed())
					})
				})

				It("does not cache events", func() {
					sendEnd()
					sendEvent()

					Eventually(table.RemoveEndpointCallCount).Should(Equal(1))
				})
			})
		})
	})
})

type unrecognizedEvent struct{}

func (u unrecognizedEvent) EventType() receptor.EventType {
	return "unrecognized-event"
}

func (u unrecognizedEvent) Key() string {
	return ""
}
