package watcher_test

import (
	"encoding/json"
	"errors"
	"os"
	"sync/atomic"
	"time"

	"github.com/cloudfoundry-incubator/bbs/events"
	"github.com/cloudfoundry-incubator/bbs/events/eventfakes"
	"github.com/cloudfoundry-incubator/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/bbs/models"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pivotal-golang/clock/fakeclock"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/tedsuo/ifrit"

	"github.com/cloudfoundry-incubator/routing-info/cfroutes"
	"github.com/cloudfoundry-incubator/route-emitter/nats_emitter/fake_nats_emitter"
	"github.com/cloudfoundry-incubator/route-emitter/routing_table"
	"github.com/cloudfoundry-incubator/route-emitter/routing_table/fake_routing_table"
	"github.com/cloudfoundry-incubator/route-emitter/syncer"
	"github.com/cloudfoundry-incubator/route-emitter/watcher"
	fake_metrics_sender "github.com/cloudfoundry/dropsonde/metric_sender/fake"
	"github.com/cloudfoundry/dropsonde/metrics"
)

const logGuid = "some-log-guid"

type EventHolder struct {
	event models.Event
}

var nilEventHolder = EventHolder{}

var _ = Describe("Watcher", func() {
	const (
		expectedProcessGuid             = "process-guid"
		expectedInstanceGuid            = "instance-guid"
		expectedHost                    = "1.1.1.1"
		expectedExternalPort            = 11000
		expectedAdditionalExternalPort  = 22000
		expectedContainerPort           = 11
		expectedAdditionalContainerPort = 22
		expectedRouteServiceUrl         = "https://so.good.com"
	)

	var (
		eventSource *eventfakes.FakeEventSource
		bbsClient   *fake_bbs.FakeClient
		table       *fake_routing_table.FakeRoutingTable
		emitter     *fake_nats_emitter.FakeNATSEmitter
		syncEvents  syncer.Events

		clock          *fakeclock.FakeClock
		watcherProcess *watcher.Watcher
		process        ifrit.Process

		expectedRoutes     []string
		expectedRoutingKey routing_table.RoutingKey
		expectedCFRoute    cfroutes.CFRoute

		expectedAdditionalRoutes     []string
		expectedAdditionalRoutingKey routing_table.RoutingKey
		expectedAdditionalCFRoute    cfroutes.CFRoute

		dummyMessagesToEmit routing_table.MessagesToEmit
		fakeMetricSender    *fake_metrics_sender.FakeMetricSender

		logger *lagertest.TestLogger

		nextErr   atomic.Value
		nextEvent atomic.Value
	)

	BeforeEach(func() {
		eventSource = new(eventfakes.FakeEventSource)
		bbsClient = new(fake_bbs.FakeClient)
		bbsClient.SubscribeToEventsReturns(eventSource, nil)

		table = &fake_routing_table.FakeRoutingTable{}
		emitter = &fake_nats_emitter.FakeNATSEmitter{}
		syncEvents = syncer.Events{
			Sync: make(chan struct{}),
			Emit: make(chan struct{}),
		}
		logger = lagertest.NewTestLogger("test")

		dummyEndpoint := routing_table.Endpoint{InstanceGuid: expectedInstanceGuid, Host: expectedHost, Port: expectedContainerPort}
		dummyMessage := routing_table.RegistryMessageFor(dummyEndpoint, routing_table.Routes{Hostnames: []string{"foo.com", "bar.com"}, LogGuid: logGuid})
		dummyMessagesToEmit = routing_table.MessagesToEmit{
			RegistrationMessages: []routing_table.RegistryMessage{dummyMessage},
		}

		clock = fakeclock.NewFakeClock(time.Now())

		watcherProcess = watcher.NewWatcher(bbsClient, clock, table, emitter, syncEvents, logger)

		expectedRoutes = []string{"route-1", "route-2"}
		expectedCFRoute = cfroutes.CFRoute{Hostnames: expectedRoutes, Port: expectedContainerPort, RouteServiceUrl: expectedRouteServiceUrl}
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
		metrics.Initialize(fakeMetricSender, nil)

		nextErr = atomic.Value{}
		nextErr := nextErr
		nextEvent.Store(nilEventHolder)

		eventSource.CloseStub = func() error {
			nextErr.Store(errors.New("closed"))
			return nil
		}

		eventSource.NextStub = func() (models.Event, error) {
			time.Sleep(10 * time.Millisecond)
			if eventHolder := nextEvent.Load(); eventHolder != nil || eventHolder != nilEventHolder {
				nextEvent.Store(nilEventHolder)

				eh := eventHolder.(EventHolder)
				if eh.event != nil {
					return eh.event, nil
				}
			}

			if err := nextErr.Load(); err != nil {
				return nil, err.(error)
			}

			return nil, nil
		}
	})

	JustBeforeEach(func() {
		process = ifrit.Invoke(watcherProcess)
	})

	AfterEach(func() {
		process.Signal(os.Interrupt)
		Eventually(process.Wait()).Should(Receive())
	})

	Context("on startup", func() {
		It("processes events after the first sync event", func() {
			Consistently(bbsClient.SubscribeToEventsCallCount).Should(Equal(0))
			syncEvents.Sync <- struct{}{}
			Eventually(bbsClient.SubscribeToEventsCallCount).Should(BeNumerically(">", 0))
		})
	})

	Describe("Desired LRP changes", func() {
		JustBeforeEach(func() {
			syncEvents.Sync <- struct{}{}
			Eventually(emitter.EmitCallCount).ShouldNot(Equal(0))
		})

		Context("when a create event occurs", func() {
			var desiredLRP *models.DesiredLRP

			BeforeEach(func() {
				routes := cfroutes.CFRoutes{expectedCFRoute}.RoutingInfo()
				desiredLRP = &models.DesiredLRP{
					Action: models.WrapAction(&models.RunAction{
						User: "me",
						Path: "ls",
					}),
					Domain:      "tests",
					ProcessGuid: expectedProcessGuid,
					Ports:       []uint32{expectedContainerPort},
					Routes:      &routes,
					LogGuid:     logGuid,
				}
			})

			JustBeforeEach(func() {
				table.SetRoutesReturns(dummyMessagesToEmit)

				nextEvent.Store(EventHolder{models.NewDesiredLRPCreatedEvent(desiredLRP)})
			})

			It("should set the routes on the table", func() {
				Eventually(table.SetRoutesCallCount).Should(Equal(1))

				key, routes := table.SetRoutesArgsForCall(0)
				Expect(key).To(Equal(expectedRoutingKey))
				Expect(routes).To(Equal(routing_table.Routes{Hostnames: expectedRoutes, LogGuid: logGuid, RouteServiceUrl: expectedRouteServiceUrl}))
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
				Eventually(emitter.EmitCallCount).Should(Equal(2))
				messagesToEmit := emitter.EmitArgsForCall(1)
				Expect(messagesToEmit).To(Equal(dummyMessagesToEmit))
			})

			Context("when there are diego ssh-keys on the route", func() {
				BeforeEach(func() {
					diegoSSHInfo := json.RawMessage([]byte(`{"ssh-key": "ssh-value"}`))

					routes := cfroutes.CFRoutes{expectedCFRoute}.RoutingInfo()
					routes["diego-ssh"] = &diegoSSHInfo

					desiredLRP.Routes = &routes
				})

				It("does not log them", func() {
					Eventually(table.SetRoutesCallCount).Should(Equal(1))
					logs := logger.Logs()

					for _, log := range logs {
						if log.Data["routes"] != nil {
							Expect(log.Data["routes"]).ToNot(HaveKey("diego-ssh"))
						}
					}

					Expect(len(*desiredLRP.Routes)).To(Equal(2))
				})
			})

			Context("when there are multiple CF routes", func() {
				BeforeEach(func() {
					routes := cfroutes.CFRoutes{expectedCFRoute, expectedAdditionalCFRoute}.RoutingInfo()
					desiredLRP.Routes = &routes
				})

				It("registers all of the routes on the table", func() {
					Eventually(table.SetRoutesCallCount).Should(Equal(2))

					key, routes := table.SetRoutesArgsForCall(0)
					Expect(key).To(Equal(expectedRoutingKey))
					Expect(routes).To(Equal(routing_table.Routes{Hostnames: expectedRoutes, LogGuid: logGuid, RouteServiceUrl: expectedRouteServiceUrl}))

					key, routes = table.SetRoutesArgsForCall(1)
					Expect(key).To(Equal(expectedAdditionalRoutingKey))
					Expect(routes).To(Equal(routing_table.Routes{Hostnames: expectedAdditionalRoutes, LogGuid: logGuid}))
				})

				It("emits whatever the table tells it to emit", func() {
					Eventually(emitter.EmitCallCount).Should(Equal(3))

					messagesToEmit := emitter.EmitArgsForCall(1)
					Expect(messagesToEmit).To(Equal(dummyMessagesToEmit))

					messagesToEmit = emitter.EmitArgsForCall(2)
					Expect(messagesToEmit).To(Equal(dummyMessagesToEmit))
				})
			})
		})

		Context("when a change event occurs", func() {
			var originalDesiredLRP, changedDesiredLRP *models.DesiredLRP

			BeforeEach(func() {
				table.SetRoutesReturns(dummyMessagesToEmit)
				routes := cfroutes.CFRoutes{{Hostnames: expectedRoutes, Port: expectedContainerPort}}.RoutingInfo()

				originalDesiredLRP = &models.DesiredLRP{
					Action: models.WrapAction(&models.RunAction{
						User: "me",
						Path: "ls",
					}),
					Domain:      "tests",
					ProcessGuid: expectedProcessGuid,
					LogGuid:     logGuid,
					Routes:      &routes,
				}
				changedDesiredLRP = &models.DesiredLRP{
					Action: models.WrapAction(&models.RunAction{
						User: "me",
						Path: "ls",
					}),
					Domain:          "tests",
					ProcessGuid:     expectedProcessGuid,
					LogGuid:         logGuid,
					Routes:          &routes,
					ModificationTag: &models.ModificationTag{Epoch: "abcd", Index: 1},
				}
			})

			JustBeforeEach(func() {
				nextEvent.Store(EventHolder{models.NewDesiredLRPChangedEvent(
					originalDesiredLRP,
					changedDesiredLRP,
				)})
			})

			It("should set the routes on the table", func() {
				Eventually(table.SetRoutesCallCount).Should(Equal(1))
				key, routes := table.SetRoutesArgsForCall(0)
				Expect(key).To(Equal(expectedRoutingKey))
				Expect(routes).To(Equal(routing_table.Routes{Hostnames: expectedRoutes, LogGuid: logGuid}))
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
				Eventually(emitter.EmitCallCount).Should(Equal(2))
				messagesToEmit := emitter.EmitArgsForCall(1)
				Expect(messagesToEmit).To(Equal(dummyMessagesToEmit))
			})

			Context("when there are diego ssh-keys on the route", func() {
				BeforeEach(func() {
					diegoSSHInfo := json.RawMessage([]byte(`{"ssh-key": "ssh-value"}`))

					routes := cfroutes.CFRoutes{expectedCFRoute}.RoutingInfo()
					routes["diego-ssh"] = &diegoSSHInfo

					changedDesiredLRP.Routes = &routes
				})

				It("does not log them", func() {
					Eventually(table.SetRoutesCallCount).Should(Equal(1))
					logs := logger.Logs()

					for _, log := range logs {
						if log.Data["routes"] != nil {
							Expect(log.Data["routes"]).ToNot(HaveKey("diego-ssh"))
						}
					}

					Expect(len(*changedDesiredLRP.Routes)).To(Equal(2))
				})
			})

			Context("when CF routes are added without an associated container port", func() {
				BeforeEach(func() {
					routes := cfroutes.CFRoutes{expectedCFRoute, expectedAdditionalCFRoute}.RoutingInfo()
					changedDesiredLRP.Routes = &routes
				})

				It("registers all of the routes associated with a port on the table", func() {
					Eventually(table.SetRoutesCallCount).Should(Equal(2))

					key, routes := table.SetRoutesArgsForCall(0)
					Expect(key).To(Equal(expectedRoutingKey))
					Expect(routes).To(Equal(routing_table.Routes{Hostnames: expectedRoutes, LogGuid: logGuid, RouteServiceUrl: expectedRouteServiceUrl}))
				})

				It("emits whatever the table tells it to emit", func() {
					Eventually(emitter.EmitCallCount).Should(Equal(3))

					messagesToEmit := emitter.EmitArgsForCall(2)
					Expect(messagesToEmit).To(Equal(dummyMessagesToEmit))
				})
			})

			Context("when CF routes and container ports are added", func() {
				BeforeEach(func() {
					routes := cfroutes.CFRoutes{expectedCFRoute, expectedAdditionalCFRoute}.RoutingInfo()
					changedDesiredLRP.Routes = &routes
				})

				It("registers all of the routes on the table", func() {
					Eventually(table.SetRoutesCallCount).Should(Equal(2))

					key, routes := table.SetRoutesArgsForCall(0)
					Expect(key).To(Equal(expectedRoutingKey))
					Expect(routes).To(Equal(routing_table.Routes{Hostnames: expectedRoutes, LogGuid: logGuid, RouteServiceUrl: expectedRouteServiceUrl}))

					key, routes = table.SetRoutesArgsForCall(1)
					Expect(key).To(Equal(expectedAdditionalRoutingKey))
					Expect(routes).To(Equal(routing_table.Routes{Hostnames: expectedAdditionalRoutes, LogGuid: logGuid}))
				})

				It("emits whatever the table tells it to emit", func() {
					Eventually(emitter.EmitCallCount).Should(Equal(3))

					messagesToEmit := emitter.EmitArgsForCall(1)
					Expect(messagesToEmit).To(Equal(dummyMessagesToEmit))

					messagesToEmit = emitter.EmitArgsForCall(2)
					Expect(messagesToEmit).To(Equal(dummyMessagesToEmit))
				})
			})

			Context("when CF routes are removed", func() {
				BeforeEach(func() {
					routes := cfroutes.CFRoutes{}.RoutingInfo()
					changedDesiredLRP.Routes = &routes

					table.SetRoutesReturns(routing_table.MessagesToEmit{})
					table.RemoveRoutesReturns(dummyMessagesToEmit)
				})

				It("deletes the routes for the missng key", func() {
					Eventually(table.RemoveRoutesCallCount).Should(Equal(1))

					key, modTag := table.RemoveRoutesArgsForCall(0)
					Expect(key).To(Equal(expectedRoutingKey))
					Expect(modTag).To(Equal(changedDesiredLRP.ModificationTag))
				})

				It("emits whatever the table tells it to emit", func() {
					Eventually(emitter.EmitCallCount).Should(Equal(2))

					messagesToEmit := emitter.EmitArgsForCall(1)
					Expect(messagesToEmit).To(Equal(dummyMessagesToEmit))
				})
			})
		})

		Context("when a delete event occurs", func() {
			var desiredLRP *models.DesiredLRP

			BeforeEach(func() {
				table.RemoveRoutesReturns(dummyMessagesToEmit)
				routes := cfroutes.CFRoutes{expectedCFRoute}.RoutingInfo()
				desiredLRP = &models.DesiredLRP{
					Action: models.WrapAction(&models.RunAction{
						User: "me",
						Path: "ls",
					}),
					Domain:          "tests",
					ProcessGuid:     expectedProcessGuid,
					Ports:           []uint32{expectedContainerPort},
					Routes:          &routes,
					LogGuid:         logGuid,
					ModificationTag: &models.ModificationTag{Epoch: "defg", Index: 2},
				}
			})

			JustBeforeEach(func() {
				nextEvent.Store(EventHolder{models.NewDesiredLRPRemovedEvent(desiredLRP)})
			})

			It("should remove the routes from the table", func() {
				Eventually(table.RemoveRoutesCallCount).Should(Equal(1))
				key, modTag := table.RemoveRoutesArgsForCall(0)
				Expect(key).To(Equal(expectedRoutingKey))
				Expect(modTag).To(Equal(desiredLRP.ModificationTag))
			})

			It("should emit whatever the table tells it to emit", func() {
				Eventually(emitter.EmitCallCount).Should(Equal(2))

				messagesToEmit := emitter.EmitArgsForCall(1)
				Expect(messagesToEmit).To(Equal(dummyMessagesToEmit))
			})

			Context("when there are diego ssh-keys on the route", func() {
				BeforeEach(func() {
					diegoSSHInfo := json.RawMessage([]byte(`{"ssh-key": "ssh-value"}`))

					routes := cfroutes.CFRoutes{expectedCFRoute}.RoutingInfo()
					routes["diego-ssh"] = &diegoSSHInfo

					desiredLRP.Routes = &routes
				})

				It("does not log them", func() {
					Eventually(table.RemoveRoutesCallCount).Should(Equal(1))
					logs := logger.Logs()

					for _, log := range logs {
						if log.Data["routes"] != nil {
							Expect(log.Data["routes"]).ToNot(HaveKey("diego-ssh"))
						}
					}
				})
			})

			Context("when there are multiple CF routes", func() {
				BeforeEach(func() {
					routes := cfroutes.CFRoutes{expectedCFRoute, expectedAdditionalCFRoute}.RoutingInfo()
					desiredLRP.Routes = &routes
				})

				It("should remove the routes from the table", func() {
					Eventually(table.RemoveRoutesCallCount).Should(Equal(2))

					key, modTag := table.RemoveRoutesArgsForCall(0)
					Expect(key).To(Equal(expectedRoutingKey))
					Expect(modTag).To(Equal(desiredLRP.ModificationTag))

					key, modTag = table.RemoveRoutesArgsForCall(1)
					Expect(key).To(Equal(expectedAdditionalRoutingKey))

					key, modTag = table.RemoveRoutesArgsForCall(0)
					Expect(key).To(Equal(expectedRoutingKey))
					Expect(modTag).To(Equal(desiredLRP.ModificationTag))
				})

				It("emits whatever the table tells it to emit", func() {
					Eventually(emitter.EmitCallCount).Should(Equal(3))

					messagesToEmit := emitter.EmitArgsForCall(1)
					Expect(messagesToEmit).To(Equal(dummyMessagesToEmit))

					messagesToEmit = emitter.EmitArgsForCall(2)
					Expect(messagesToEmit).To(Equal(dummyMessagesToEmit))
				})
			})
		})
	})

	Describe("Actual LRP changes", func() {
		JustBeforeEach(func() {
			syncEvents.Sync <- struct{}{}
			Eventually(emitter.EmitCallCount).ShouldNot(Equal(0))
		})

		Context("when a create event occurs", func() {
			var (
				actualLRPGroup       *models.ActualLRPGroup
				actualLRP            *models.ActualLRP
				actualLRPRoutingInfo *routing_table.ActualLRPRoutingInfo
			)

			Context("when the resulting LRP is in the RUNNING state", func() {
				BeforeEach(func() {
					actualLRP = &models.ActualLRP{
						ActualLRPKey:         models.NewActualLRPKey(expectedProcessGuid, 1, "domain"),
						ActualLRPInstanceKey: models.NewActualLRPInstanceKey(expectedInstanceGuid, "cell-id"),
						ActualLRPNetInfo: models.NewActualLRPNetInfo(
							expectedHost,
							models.NewPortMapping(expectedExternalPort, expectedContainerPort),
							models.NewPortMapping(expectedExternalPort, expectedAdditionalContainerPort),
						),
						State: models.ActualLRPStateRunning,
					}

					actualLRPGroup = &models.ActualLRPGroup{
						Instance: actualLRP,
					}

					actualLRPRoutingInfo = &routing_table.ActualLRPRoutingInfo{
						ActualLRP:  actualLRP,
						Evacuating: false,
					}
				})

				JustBeforeEach(func() {
					table.AddEndpointReturns(dummyMessagesToEmit)
					nextEvent.Store(EventHolder{models.NewActualLRPCreatedEvent(actualLRPGroup)})
				})

				It("should add/update the endpoints on the table", func() {
					Eventually(table.AddEndpointCallCount).Should(Equal(2))

					keys := routing_table.RoutingKeysFromActual(actualLRP)
					endpoints, err := routing_table.EndpointsFromActual(actualLRPRoutingInfo)
					Expect(err).NotTo(HaveOccurred())

					key, endpoint := table.AddEndpointArgsForCall(0)
					Expect(keys).To(ContainElement(key))
					Expect(endpoint).To(Equal(endpoints[key.ContainerPort]))

					key, endpoint = table.AddEndpointArgsForCall(1)
					Expect(keys).To(ContainElement(key))
					Expect(endpoint).To(Equal(endpoints[key.ContainerPort]))
				})

				It("should emit whatever the table tells it to emit", func() {
					Eventually(emitter.EmitCallCount).Should(Equal(3))

					messagesToEmit := emitter.EmitArgsForCall(1)
					Expect(messagesToEmit).To(Equal(dummyMessagesToEmit))
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
				JustBeforeEach(func() {
					actualLRP = &models.ActualLRP{
						ActualLRPKey:         models.NewActualLRPKey(expectedProcessGuid, 1, "domain"),
						ActualLRPInstanceKey: models.NewActualLRPInstanceKey(expectedInstanceGuid, "cell-id"),
						ActualLRPNetInfo: models.NewActualLRPNetInfo(
							expectedHost,
							models.NewPortMapping(expectedExternalPort, expectedContainerPort),
							models.NewPortMapping(expectedExternalPort, expectedAdditionalContainerPort),
						),
						State: models.ActualLRPStateUnclaimed,
					}

					actualLRPGroup = &models.ActualLRPGroup{
						Instance: actualLRP,
					}
					nextEvent.Store(EventHolder{models.NewActualLRPCreatedEvent(actualLRPGroup)})
				})

				It("doesn't add/update the endpoint on the table", func() {
					Consistently(table.AddEndpointCallCount).Should(Equal(0))
				})

				It("doesn't emit", func() {
					Eventually(emitter.EmitCallCount).Should(Equal(1))
				})
			})
		})

		Context("when a change event occurs", func() {
			Context("when the resulting LRP is in the RUNNING state", func() {
				BeforeEach(func() {
					table.AddEndpointReturns(dummyMessagesToEmit)
				})

				JustBeforeEach(func() {
					beforeActualLRP := &models.ActualLRPGroup{
						Instance: &models.ActualLRP{
							ActualLRPKey:         models.NewActualLRPKey(expectedProcessGuid, 1, "domain"),
							ActualLRPInstanceKey: models.NewActualLRPInstanceKey(expectedInstanceGuid, "cell-id"),
							State:                models.ActualLRPStateClaimed,
						},
					}
					afterActualLRP := &models.ActualLRPGroup{
						Instance: &models.ActualLRP{
							ActualLRPKey:         models.NewActualLRPKey(expectedProcessGuid, 1, "domain"),
							ActualLRPInstanceKey: models.NewActualLRPInstanceKey(expectedInstanceGuid, "cell-id"),
							ActualLRPNetInfo: models.NewActualLRPNetInfo(
								expectedHost,
								models.NewPortMapping(expectedExternalPort, expectedContainerPort),
								models.NewPortMapping(expectedAdditionalExternalPort, expectedAdditionalContainerPort),
							),
							State: models.ActualLRPStateRunning,
						},
					}

					nextEvent.Store(EventHolder{models.NewActualLRPChangedEvent(beforeActualLRP, afterActualLRP)})
				})

				It("should add/update the endpoint on the table", func() {
					Eventually(table.AddEndpointCallCount).Should(Equal(2))

					key, endpoint := table.AddEndpointArgsForCall(0)
					Expect(key).To(Equal(expectedRoutingKey))
					Expect(endpoint).To(Equal(routing_table.Endpoint{
						InstanceGuid:  expectedInstanceGuid,
						Host:          expectedHost,
						Port:          expectedExternalPort,
						ContainerPort: expectedContainerPort,
					}))

					key, endpoint = table.AddEndpointArgsForCall(1)
					Expect(key).To(Equal(expectedAdditionalRoutingKey))
					Expect(endpoint).To(Equal(routing_table.Endpoint{
						InstanceGuid:  expectedInstanceGuid,
						Host:          expectedHost,
						Port:          expectedAdditionalExternalPort,
						ContainerPort: expectedAdditionalContainerPort,
					}))

				})

				It("should emit whatever the table tells it to emit", func() {
					Eventually(emitter.EmitCallCount).Should(Equal(3))

					messagesToEmit := emitter.EmitArgsForCall(1)
					Expect(messagesToEmit).To(Equal(dummyMessagesToEmit))
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

			Context("when the resulting LRP transitions away from the RUNNING state", func() {
				JustBeforeEach(func() {
					table.RemoveEndpointReturns(dummyMessagesToEmit)
					beforeActualLRP := &models.ActualLRPGroup{
						Instance: &models.ActualLRP{
							ActualLRPKey:         models.NewActualLRPKey(expectedProcessGuid, 1, "domain"),
							ActualLRPInstanceKey: models.NewActualLRPInstanceKey(expectedInstanceGuid, "cell-id"),
							ActualLRPNetInfo: models.NewActualLRPNetInfo(
								expectedHost,
								models.NewPortMapping(expectedExternalPort, expectedContainerPort),
								models.NewPortMapping(expectedAdditionalExternalPort, expectedAdditionalContainerPort),
							),
							State: models.ActualLRPStateRunning,
						},
					}
					afterActualLRP := &models.ActualLRPGroup{
						Instance: &models.ActualLRP{
							ActualLRPKey: models.NewActualLRPKey(expectedProcessGuid, 1, "domain"),
							State:        models.ActualLRPStateUnclaimed,
						},
					}

					nextEvent.Store(EventHolder{models.NewActualLRPChangedEvent(beforeActualLRP, afterActualLRP)})
				})

				It("should remove the endpoint from the table", func() {
					Eventually(table.RemoveEndpointCallCount).Should(Equal(2))

					key, endpoint := table.RemoveEndpointArgsForCall(0)
					Expect(key).To(Equal(expectedRoutingKey))
					Expect(endpoint).To(Equal(routing_table.Endpoint{
						InstanceGuid:  expectedInstanceGuid,
						Host:          expectedHost,
						Port:          expectedExternalPort,
						ContainerPort: expectedContainerPort,
					}))

					key, endpoint = table.RemoveEndpointArgsForCall(1)
					Expect(key).To(Equal(expectedAdditionalRoutingKey))
					Expect(endpoint).To(Equal(routing_table.Endpoint{
						InstanceGuid:  expectedInstanceGuid,
						Host:          expectedHost,
						Port:          expectedAdditionalExternalPort,
						ContainerPort: expectedAdditionalContainerPort,
					}))

				})

				It("should emit whatever the table tells it to emit", func() {
					Eventually(emitter.EmitCallCount).Should(Equal(3))

					messagesToEmit := emitter.EmitArgsForCall(1)
					Expect(messagesToEmit).To(Equal(dummyMessagesToEmit))
				})
			})

			Context("when the endpoint neither starts nor ends in the RUNNING state", func() {
				JustBeforeEach(func() {
					beforeActualLRP := &models.ActualLRPGroup{
						Instance: &models.ActualLRP{
							ActualLRPKey: models.NewActualLRPKey(expectedProcessGuid, 1, "domain"),
							State:        models.ActualLRPStateUnclaimed,
						},
					}
					afterActualLRP := &models.ActualLRPGroup{
						Instance: &models.ActualLRP{
							ActualLRPKey:         models.NewActualLRPKey(expectedProcessGuid, 1, "domain"),
							ActualLRPInstanceKey: models.NewActualLRPInstanceKey(expectedInstanceGuid, "cell-id"),
							State:                models.ActualLRPStateClaimed,
						},
					}
					nextEvent.Store(EventHolder{models.NewActualLRPChangedEvent(beforeActualLRP, afterActualLRP)})
				})

				It("should not remove the endpoint", func() {
					Consistently(table.RemoveEndpointCallCount).Should(BeZero())
				})

				It("should not add or update the endpoint", func() {
					Consistently(table.AddEndpointCallCount).Should(BeZero())
				})

				It("should not emit anything", func() {
					Consistently(emitter.EmitCallCount).Should(Equal(1))
				})
			})
		})

		Context("when a delete event occurs", func() {
			Context("when the actual is in the RUNNING state", func() {
				BeforeEach(func() {
					table.RemoveEndpointReturns(dummyMessagesToEmit)
				})

				JustBeforeEach(func() {
					actualLRP := &models.ActualLRPGroup{
						Instance: &models.ActualLRP{
							ActualLRPKey:         models.NewActualLRPKey(expectedProcessGuid, 1, "domain"),
							ActualLRPInstanceKey: models.NewActualLRPInstanceKey(expectedInstanceGuid, "cell-id"),
							ActualLRPNetInfo: models.NewActualLRPNetInfo(
								expectedHost,
								models.NewPortMapping(expectedExternalPort, expectedContainerPort),
								models.NewPortMapping(expectedAdditionalExternalPort, expectedAdditionalContainerPort),
							),
							State: models.ActualLRPStateRunning,
						},
					}

					nextEvent.Store(EventHolder{models.NewActualLRPRemovedEvent(actualLRP)})
				})

				It("should remove the endpoint from the table", func() {
					Eventually(table.RemoveEndpointCallCount).Should(Equal(2))

					key, endpoint := table.RemoveEndpointArgsForCall(0)
					Expect(key).To(Equal(expectedRoutingKey))
					Expect(endpoint).To(Equal(routing_table.Endpoint{
						InstanceGuid:  expectedInstanceGuid,
						Host:          expectedHost,
						Port:          expectedExternalPort,
						ContainerPort: expectedContainerPort,
					}))

					key, endpoint = table.RemoveEndpointArgsForCall(1)
					Expect(key).To(Equal(expectedAdditionalRoutingKey))
					Expect(endpoint).To(Equal(routing_table.Endpoint{
						InstanceGuid:  expectedInstanceGuid,
						Host:          expectedHost,
						Port:          expectedAdditionalExternalPort,
						ContainerPort: expectedAdditionalContainerPort,
					}))

				})

				It("should emit whatever the table tells it to emit", func() {
					Eventually(emitter.EmitCallCount).Should(Equal(3))

					messagesToEmit := emitter.EmitArgsForCall(1)
					Expect(messagesToEmit).To(Equal(dummyMessagesToEmit))

					messagesToEmit = emitter.EmitArgsForCall(2)
					Expect(messagesToEmit).To(Equal(dummyMessagesToEmit))
				})
			})

			Context("when the actual is not in the RUNNING state", func() {
				JustBeforeEach(func() {
					actualLRP := &models.ActualLRPGroup{
						Instance: &models.ActualLRP{
							ActualLRPKey: models.NewActualLRPKey(expectedProcessGuid, 1, "domain"),
							State:        models.ActualLRPStateCrashed,
						},
					}

					nextEvent.Store(EventHolder{models.NewActualLRPRemovedEvent(actualLRP)})
				})

				It("doesn't remove the endpoint from the table", func() {
					Consistently(table.RemoveEndpointCallCount).Should(Equal(0))
				})

				It("doesn't emit", func() {
					Consistently(emitter.EmitCallCount).Should(Equal(1))
				})
			})
		})
	})

	Describe("Unrecognized events", func() {
		BeforeEach(func() {
			nextEvent.Store(EventHolder{&unrecognizedEvent{}})
		})

		JustBeforeEach(func() {
			syncEvents.Sync <- struct{}{}
			Eventually(emitter.EmitCallCount).Should(Equal(1))
		})

		It("does not emit any more messages", func() {
			Consistently(emitter.EmitCallCount).Should(Equal(1))
		})
	})

	Context("when the event source returns an error", func() {
		var subscribeErr error

		BeforeEach(func() {
			subscribeErr = errors.New("subscribe-error")

			bbsClient.SubscribeToEventsStub = func() (events.EventSource, error) {
				if bbsClient.SubscribeToEventsCallCount() == 1 {
					return eventSource, nil
				}
				return nil, subscribeErr
			}

			eventSource.NextStub = func() (models.Event, error) {
				return nil, errors.New("next-error")
			}
		})

		JustBeforeEach(func() {
			syncEvents.Sync <- struct{}{}
		})

		It("re-subscribes", func() {
			Eventually(bbsClient.SubscribeToEventsCallCount, 2*time.Second).Should(BeNumerically(">", 5))
		})

		It("does not exit", func() {
			Consistently(process.Wait()).ShouldNot(Receive())
		})
	})

	Describe("interrupting the process", func() {
		It("should be possible to SIGINT the route emitter", func() {
			process.Signal(os.Interrupt)
			Eventually(process.Wait()).Should(Receive())
		})
	})

	Describe("Sync Events", func() {
		var nextEvent chan models.Event

		BeforeEach(func() {
			nextEvent = make(chan models.Event)

			nextEvent := nextEvent
			nextErr := nextErr
			eventSource.NextStub = func() (models.Event, error) {
				select {
				case e := <-nextEvent:
					return e, nil
				default:
				}

				if err := nextErr.Load(); err != nil {
					return nil, err.(error)
				}

				return nil, nil
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
				Expect(emitter.EmitArgsForCall(0)).To(Equal(dummyMessagesToEmit))
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
			currentTag := &models.ModificationTag{Epoch: "abc", Index: 1}
			hostname1 := "foo.example.com"
			hostname2 := "bar.example.com"
			endpoint1 := routing_table.Endpoint{InstanceGuid: "ig-1", Host: "1.1.1.1", Port: 11, ContainerPort: 8080, Evacuating: false, ModificationTag: currentTag}
			endpoint2 := routing_table.Endpoint{InstanceGuid: "ig-2", Host: "2.2.2.2", Port: 22, ContainerPort: 8080, Evacuating: false, ModificationTag: currentTag}

			schedulingInfo1 := &models.DesiredLRPSchedulingInfo{
				DesiredLRPKey: models.NewDesiredLRPKey("pg-1", "tests", "lg1"),
				Routes: cfroutes.CFRoutes{
					cfroutes.CFRoute{
						Hostnames:       []string{hostname1},
						Port:            8080,
						RouteServiceUrl: "https://rs.example.com",
					},
				}.RoutingInfo(),
			}

			schedulingInfo2 := &models.DesiredLRPSchedulingInfo{
				DesiredLRPKey: models.NewDesiredLRPKey("pg-2", "tests", "lg2"),
				Routes: cfroutes.CFRoutes{
					cfroutes.CFRoute{
						Hostnames: []string{hostname2},
						Port:      8080,
					},
				}.RoutingInfo(),
			}

			actualLRPGroup1 := &models.ActualLRPGroup{
				Instance: &models.ActualLRP{
					ActualLRPKey:         models.NewActualLRPKey("pg-1", 1, "domain"),
					ActualLRPInstanceKey: models.NewActualLRPInstanceKey(endpoint1.InstanceGuid, "cell-id"),
					ActualLRPNetInfo:     models.NewActualLRPNetInfo(endpoint1.Host, models.NewPortMapping(endpoint1.Port, endpoint1.ContainerPort)),
					State:                models.ActualLRPStateRunning,
				},
			}

			actualLRPGroup2 := &models.ActualLRPGroup{
				Instance: &models.ActualLRP{
					ActualLRPKey:         models.NewActualLRPKey("pg-2", 1, "domain"),
					ActualLRPInstanceKey: models.NewActualLRPInstanceKey(endpoint2.InstanceGuid, "cell-id"),
					ActualLRPNetInfo:     models.NewActualLRPNetInfo(endpoint2.Host, models.NewPortMapping(endpoint2.Port, endpoint2.ContainerPort)),
					State:                models.ActualLRPStateRunning,
				},
			}

			sendEvent := func() {
				nextEvent <- models.NewActualLRPRemovedEvent(actualLRPGroup1)
			}

			Context("when sync begins", func() {
				JustBeforeEach(func() {
					syncEvents.Sync <- struct{}{}
				})

				Describe("bbs events", func() {
					var ready chan struct{}
					var count int32

					BeforeEach(func() {
						ready = make(chan struct{})
						count = 0

						bbsClient.ActualLRPGroupsStub = func(filter models.ActualLRPFilter) ([]*models.ActualLRPGroup, error) {
							defer GinkgoRecover()

							atomic.AddInt32(&count, 1)
							ready <- struct{}{}
							Eventually(ready).Should(Receive())
							return nil, nil
						}
					})

					JustBeforeEach(func() {
						Eventually(ready).Should(Receive())
					})

					It("caches events", func() {
						sendEvent()
						Consistently(table.RemoveEndpointCallCount).Should(Equal(0))
						ready <- struct{}{}
					})

					Context("additional sync events", func() {
						JustBeforeEach(func() {
							syncEvents.Sync <- struct{}{}
						})

						It("ignores the sync event", func() {
							Consistently(atomic.LoadInt32(&count)).Should(Equal(int32(1)))
							ready <- struct{}{}
						})
					})
				})

				Context("when fetching actuals fails", func() {
					var returnError int32

					BeforeEach(func() {
						returnError = 1

						bbsClient.ActualLRPGroupsStub = func(filter models.ActualLRPFilter) ([]*models.ActualLRPGroup, error) {
							if atomic.LoadInt32(&returnError) == 1 {
								return nil, errors.New("bam")
							}

							return []*models.ActualLRPGroup{}, nil
						}
					})

					It("should not call sync until the error resolves", func() {
						Eventually(bbsClient.ActualLRPGroupsCallCount).Should(Equal(1))
						Consistently(table.SwapCallCount).Should(Equal(0))

						atomic.StoreInt32(&returnError, 0)
						syncEvents.Sync <- struct{}{}

						Eventually(table.SwapCallCount).Should(Equal(1))
						Expect(bbsClient.ActualLRPGroupsCallCount()).To(Equal(2))
					})
				})

				Context("when fetching desireds fails", func() {
					var returnError int32

					BeforeEach(func() {
						returnError = 1

						bbsClient.DesiredLRPSchedulingInfosStub = func(filter models.DesiredLRPFilter) ([]*models.DesiredLRPSchedulingInfo, error) {
							if atomic.LoadInt32(&returnError) == 1 {
								return nil, errors.New("bam")
							}

							return []*models.DesiredLRPSchedulingInfo{}, nil
						}
					})

					It("should not call sync until the error resolves", func() {
						Eventually(bbsClient.DesiredLRPSchedulingInfosCallCount).Should(Equal(1))
						Consistently(table.SwapCallCount).Should(Equal(0))

						atomic.StoreInt32(&returnError, 0)
						syncEvents.Sync <- struct{}{}

						Eventually(table.SwapCallCount).Should(Equal(1))
						Expect(bbsClient.DesiredLRPSchedulingInfosCallCount()).To(Equal(2))
					})
				})
			})

			Context("when syncing ends", func() {
				BeforeEach(func() {
					bbsClient.ActualLRPGroupsStub = func(f models.ActualLRPFilter) ([]*models.ActualLRPGroup, error) {
						clock.IncrementBySeconds(1)

						return []*models.ActualLRPGroup{
							actualLRPGroup1,
							actualLRPGroup2,
						}, nil
					}
				})

				JustBeforeEach(func() {
					syncEvents.Sync <- struct{}{}
				})

				It("swaps the tables", func() {
					Eventually(table.SwapCallCount).Should(Equal(1))
				})

				Context("a table with a single routable endpoint", func() {
					var ready chan struct{}

					BeforeEach(func() {
						ready = make(chan struct{})

						actualLRPRoutingInfo1 := &routing_table.ActualLRPRoutingInfo{
							ActualLRP:  actualLRPGroup1.Instance,
							Evacuating: false,
						}

						actualLRPRoutingInfo2 := &routing_table.ActualLRPRoutingInfo{
							ActualLRP:  actualLRPGroup2.Instance,
							Evacuating: false,
						}

						tempTable := routing_table.NewTempTable(
							routing_table.RoutesByRoutingKeyFromSchedulingInfos([]*models.DesiredLRPSchedulingInfo{schedulingInfo1, schedulingInfo2}),
							routing_table.EndpointsByRoutingKeyFromActuals([]*routing_table.ActualLRPRoutingInfo{
								actualLRPRoutingInfo1,
								actualLRPRoutingInfo2,
							}),
						)

						table := routing_table.NewTable()
						table.Swap(tempTable)

						watcherProcess = watcher.NewWatcher(bbsClient, clock, table, emitter, syncEvents, logger)

						bbsClient.DesiredLRPSchedulingInfosStub = func(f models.DesiredLRPFilter) ([]*models.DesiredLRPSchedulingInfo, error) {
							defer GinkgoRecover()

							ready <- struct{}{}
							Eventually(ready).Should(Receive())

							return []*models.DesiredLRPSchedulingInfo{schedulingInfo1, schedulingInfo2}, nil
						}
					})

					It("applies the cached events and emits", func() {
						Eventually(ready).Should(Receive())
						sendEvent()

						ready <- struct{}{}

						Eventually(emitter.EmitCallCount).Should(Equal(1))
						Expect(emitter.EmitArgsForCall(0)).To(Equal(routing_table.MessagesToEmit{
							RegistrationMessages: []routing_table.RegistryMessage{
								routing_table.RegistryMessageFor(endpoint2, routing_table.Routes{Hostnames: []string{hostname2}, LogGuid: "lg2"}),
							},
							UnregistrationMessages: []routing_table.RegistryMessage{
								routing_table.RegistryMessageFor(endpoint1, routing_table.Routes{Hostnames: []string{hostname1}, LogGuid: "lg1", RouteServiceUrl: "https://rs.example.com"}),
							},
						}))
					})
				})

				It("should emit the sync duration, and allow event processing", func() {
					Eventually(func() float64 {
						return fakeMetricSender.GetValue("RouteEmitterSyncDuration").Value
					}).Should(BeNumerically(">=", 100*time.Millisecond))

					By("completing, events are no longer cached")
					sendEvent()

					Eventually(table.RemoveEndpointCallCount).Should(Equal(1))
				})
			})
		})
	})
})

type unrecognizedEvent struct{}

func (u *unrecognizedEvent) EventType() string { return "unrecognized-event" }
func (u *unrecognizedEvent) Key() string       { return "" }
func (u *unrecognizedEvent) Reset()            {}
func (u *unrecognizedEvent) ProtoMessage()     {}
func (u *unrecognizedEvent) String() string    { return "unrecognized-event" }
