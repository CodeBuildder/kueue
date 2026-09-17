/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package rayjob

import (
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	"sigs.k8s.io/kueue/pkg/controller/jobs/ray"
	"sigs.k8s.io/kueue/pkg/util/api"
)

var _ jobframework.MultiKueueAdapter = ray.NewMKAdapter(
	copyJobSpec, copyJobStatus, getEmptyList, gvk, getManagedBy, setManagedBy,
	ray.WithMarkInactiveOnDelete(markInactive),
)

// markInactive sets the manager RayJob's mirrored deployment status to
// Suspended once MultiKueue has confirmed its remote copy is gone - see
// ray.WithMarkInactiveOnDelete.
//
// Deletion also happens on the plain, successful path: once a RayJob finishes
// and its Workload has no quota reservation left, MultiKueue deletes the
// remote copy as routine cleanup - by then the manager already mirrored the
// real terminal status (Complete, Failed, ValidationFailed) from the remote
// before it went away. Overwriting that with Suspended would replace a
// correct, informative status with a misleading one, so this only steps in
// when the mirrored status isn't already terminal - the same condition the
// stale-forever bug requires in the first place.
func markInactive(job *rayv1.RayJob) {
	switch job.Status.JobDeploymentStatus {
	case rayv1.JobDeploymentStatusComplete, rayv1.JobDeploymentStatusFailed, rayv1.JobDeploymentStatusValidationFailed:
		return
	}
	job.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusSuspended
}

func copyJobStatus(dst, src *rayv1.RayJob) {
	dst.Status = src.Status
}

func copyJobSpec(dst, src *rayv1.RayJob) {
	*dst = rayv1.RayJob{
		ObjectMeta: api.CloneObjectMetaForCreation(&src.ObjectMeta),
		Spec:       *src.Spec.DeepCopy(),
	}
}

func getEmptyList() client.ObjectList {
	return &rayv1.RayJobList{}
}

func getManagedBy(job *rayv1.RayJob) *string {
	return job.Spec.ManagedBy
}

func setManagedBy(job *rayv1.RayJob, val *string) {
	job.Spec.ManagedBy = val
}
