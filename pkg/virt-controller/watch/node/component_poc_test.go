package node

import (
	"context"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"

	k8sv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	framework "k8s.io/client-go/tools/cache/testing"
	"k8s.io/client-go/tools/record"

	v1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/kubecli"
	kubevirtfake "kubevirt.io/client-go/kubevirt/fake"

	controllertesting "kubevirt.io/kubevirt/pkg/controller/testing"
	"kubevirt.io/kubevirt/pkg/testutils"
	watchtesting "kubevirt.io/kubevirt/pkg/virt-controller/watch/testing"
)

// nodeComponentFixture bundles test infrastructure for node controller component tests.
// It wires fake clientsets, mock routing, informers, and a work queue into a single
// struct so that each test only needs to seed state and assert on outcomes.
type nodeComponentFixture struct {
	ctrl           *gomock.Controller
	controller     *Controller
	virtClientMock *kubecli.MockKubevirtClient
	virtClient     *kubevirtfake.Clientset
	kubeClient     *k8sfake.Clientset
	nodeSource     *framework.FakeControllerSource
	vmiSource      *framework.FakeControllerSource
	nodeFeeder     *testutils.ResourceSourceFeeder[string]
	vmiFeeder      *testutils.ResourceSourceFeeder[string]
	recorder       *record.FakeRecorder
	stop           chan struct{}
}

func newNodeComponentFixture() *nodeComponentFixture {
	fixture := &nodeComponentFixture{
		ctrl: gomock.NewController(GinkgoT()),
		stop: make(chan struct{}),
	}

	fixture.virtClientMock = kubecli.NewMockKubevirtClient(fixture.ctrl)
	fixture.virtClient = kubevirtfake.NewSimpleClientset()
	fixture.kubeClient = k8sfake.NewSimpleClientset()

	nodeInformer, nodeSource := testutils.NewFakeInformerFor(&k8sv1.Node{})
	vmiInformer, vmiSource := testutils.NewFakeInformerFor(&v1.VirtualMachineInstance{})
	fixture.nodeSource = nodeSource
	fixture.vmiSource = vmiSource

	recorder := testutils.NewComponentRecorder(100)
	fixture.recorder = recorder

	controller, err := NewController(fixture.virtClientMock, nodeInformer, vmiInformer, recorder)
	Expect(err).ToNot(HaveOccurred())
	fixture.controller = controller

	queue := testutils.NewMockWorkQueue(fixture.controller.Queue)
	fixture.controller.Queue = queue
	fixture.nodeFeeder = testutils.NewResourceSourceFeeder(queue, fixture.nodeSource)
	fixture.vmiFeeder = testutils.NewResourceSourceFeeder(queue, fixture.vmiSource)

	// Route MockKubevirtClient methods to the fake clientsets.
	// These use .AnyTimes() because we assert on final state, not call counts.
	fixture.virtClientMock.EXPECT().VirtualMachineInstance(metav1.NamespaceAll).Return(fixture.virtClient.KubevirtV1().VirtualMachineInstances(metav1.NamespaceAll)).AnyTimes()
	fixture.virtClientMock.EXPECT().VirtualMachineInstance(metav1.NamespaceDefault).Return(fixture.virtClient.KubevirtV1().VirtualMachineInstances(metav1.NamespaceDefault)).AnyTimes()
	fixture.virtClientMock.EXPECT().CoreV1().Return(fixture.kubeClient.CoreV1()).AnyTimes()
	fixture.virtClientMock.EXPECT().AppsV1().Return(fixture.kubeClient.AppsV1()).AnyTimes()

	Expect(testutils.StartInformersAndWaitForCacheSync(fixture.stop, nodeInformer, vmiInformer)).To(BeTrue())

	return fixture
}

// createVMIInFakeClient seeds a VMI into the fake kubevirt clientset so that
// subsequent Get/List calls on the fake client return it. This does NOT add the
// VMI to the controller's informer store — use vmiFeeder.Add() for that.
func (f *nodeComponentFixture) createVMIInFakeClient(vmi *v1.VirtualMachineInstance) {
	_, err := f.virtClient.KubevirtV1().VirtualMachineInstances(vmi.Namespace).Create(context.Background(), vmi, metav1.CreateOptions{})
	Expect(err).ToNot(HaveOccurred())
}

// SanityExecute runs the controller's Execute method twice and asserts idempotency:
// the second run should produce no new actions if no state has changed.
func (f *nodeComponentFixture) SanityExecute() {
	controllertesting.SanityExecute(f.controller, []cache.Store{f.controller.vmiStore, f.controller.nodeStore}, Default)
}

// expectVMIFailedWithReason asserts that the VMI in the fake client has transitioned
// to Failed phase with the NodeUnresponsiveReason.
func (f *nodeComponentFixture) expectVMIFailedWithReason(vmiName string) {
	updatedVMI, err := f.virtClient.KubevirtV1().VirtualMachineInstances(metav1.NamespaceDefault).Get(context.Background(), vmiName, metav1.GetOptions{})
	Expect(err).ToNot(HaveOccurred())
	Expect(updatedVMI.Status.Phase).To(Equal(v1.Failed))
	Expect(updatedVMI.Status.Reason).To(Equal(NodeUnresponsiveReason))
}

// Close shuts down informers, finishes the gomock controller, and asserts no
// unconsumed events remain in the recorder.
func (f *nodeComponentFixture) Close() {
	close(f.stop)
	f.ctrl.Finish()
	Expect(f.recorder.Events).To(BeEmpty())
}

var _ = Describe("Node controller component tests", func() {
	var fixture *nodeComponentFixture

	BeforeEach(func() {
		fixture = newNodeComponentFixture()
	})

	AfterEach(func() {
		fixture.Close()
	})

	It("marks an unresponsive node as unschedulable", func() {
		node := NewHealthyNode("testnode")
		node.Annotations[v1.VirtHandlerHeartbeat] = nowAsJSONWithOffset(-10 * time.Minute)

		fixture.nodeFeeder.Add(node)

		fixture.kubeClient.Fake.PrependReactor("patch", "nodes", func(action k8stesting.Action) (bool, runtime.Object, error) {
			patchAction, ok := action.(k8stesting.PatchAction)
			Expect(ok).To(BeTrue())
			Expect(string(patchAction.GetPatch())).To(Equal(`{"metadata": { "labels": {"kubevirt.io/schedulable": "false"}}}`))
			return true, nil, nil
		})

		fixture.SanityExecute()
		testutils.ExpectEvent(fixture.recorder, NodeUnresponsiveReason)
	})

	It("transitions VMI to failed when its node is unhealthy and has no virt-launcher pod", func() {
		node := NewUnhealthyNode("testnode")
		vmi := watchtesting.NewRunningVirtualMachine("testvmi", node)

		// Seed VMI into the fake client so that the controller's status update
		// (via the mock → fake client chain) targets an existing object.
		fixture.createVMIInFakeClient(vmi)

		// Add node to the informer store directly (without enqueuing) because
		// this test targets VMI-triggered reconciliation, not node-triggered.
		Expect(fixture.controller.nodeStore.Add(node)).To(Succeed())

		// Enqueue the VMI — this is the event that triggers reconciliation.
		fixture.vmiFeeder.Add(vmi)

		fixture.kubeClient.Fake.PrependReactor("list", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
			listAction, _ := action.(k8stesting.ListAction)
			if strings.Contains(listAction.GetListRestrictions().Labels.String(), "virt-handler") {
				return true, &k8sv1.PodList{Items: []k8sv1.Pod{*NewVirtHandlerPod(node.Name)}}, nil
			}

			return true, &k8sv1.PodList{}, nil
		})

		fixture.SanityExecute()
		testutils.ExpectEvent(fixture.recorder, NodeUnresponsiveReason)
		fixture.expectVMIFailedWithReason(vmi.Name)
	})

	It("does nothing for responsive nodes", func() {
		fixture.nodeFeeder.Add(NewHealthyNode("testnode"))
		fixture.SanityExecute()
	})
})
