package testutils

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
	framework "k8s.io/client-go/tools/cache/testing"
	"k8s.io/client-go/tools/record"
)

func NewComponentRecorder(buffer int) *record.FakeRecorder {
	recorder := record.NewFakeRecorder(buffer)
	recorder.IncludeObject = true
	return recorder
}

func StartInformersAndWaitForCacheSync(stop <-chan struct{}, informers ...cache.SharedIndexInformer) bool {
	for _, informer := range informers {
		go informer.Run(stop)
	}

	syncedChecks := make([]cache.InformerSynced, 0, len(informers))
	for _, informer := range informers {
		syncedChecks = append(syncedChecks, informer.HasSynced)
	}

	return cache.WaitForCacheSync(stop, syncedChecks...)
}

type ResourceSourceFeeder[T comparable] struct {
	MockQueue *MockWorkQueue[T]
	Source    *framework.FakeControllerSource
}

func (f *ResourceSourceFeeder[T]) Add(obj runtime.Object) {
	f.MockQueue.ExpectAdds(1)
	f.Source.Add(obj)
	f.MockQueue.Wait()
}

func (f *ResourceSourceFeeder[T]) Modify(obj runtime.Object) {
	f.MockQueue.ExpectAdds(1)
	f.Source.Modify(obj)
	f.MockQueue.Wait()
}

func (f *ResourceSourceFeeder[T]) Delete(obj runtime.Object) {
	f.MockQueue.ExpectAdds(1)
	f.Source.Delete(obj)
	f.MockQueue.Wait()
}

func NewResourceSourceFeeder[T comparable](queue *MockWorkQueue[T], source *framework.FakeControllerSource) *ResourceSourceFeeder[T] {
	return &ResourceSourceFeeder[T]{
		MockQueue: queue,
		Source:    source,
	}
}
