// Copyright (c) 2026 Huawei Technologies Co., Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"runtime-launcher/api/proto/runtime/v1"
	"runtime-launcher/internal/runtime"
	"runtime-launcher/internal/state"
)

const (
	defaultCPUMillicore    = 500.0
	defaultMemoryMB        = 512.0
	runtimeLabelSlots      = 2
	cpuQuotaMultiplier     = 100
	endpointCleanupTimeout = 5 * time.Second
)

// LauncherService implements sandboxd-compatible runtime.v1.SandboxService.
type LauncherService struct {
	runtimev1.UnimplementedSandboxServiceServer

	runtime  runtime.ContainerRuntime
	stateMgr *state.Manager
}

// NewLauncherService creates a SandboxService facade over a container runtime.
func NewLauncherService(runtime runtime.ContainerRuntime, stateMgr *state.Manager) *LauncherService {
	return &LauncherService{runtime: runtime, stateMgr: stateMgr}
}

// Start creates a sandbox container and starts watching its exit status.
func (s *LauncherService) Start(
	ctx context.Context,
	req *runtimev1.StartRequest,
) (*runtimev1.StartResponse, error) {
	if req == nil {
		return &runtimev1.StartResponse{Code: 1, Message: "sandbox request cannot be nil"}, nil
	}
	cfg := s.buildCreateConfig(req)
	log.Printf(
		"[service] Start: sandbox_id=%s runtime=%s image=%s",
		req.GetSandboxId(),
		req.GetRuntime(),
		cfg.Sandbox,
	)
	return s.startWithConfig(ctx, cfg)
}

// Restore reports that runtime-launcher does not implement sandbox restore.
// The v3 wire contract routes Restore through RestoreResponse; the stub keeps
// that return type instead of the legacy StartResponse shape.
func (s *LauncherService) Restore(
	ctx context.Context,
	req *runtimev1.RestoreRequest,
) (*runtimev1.RestoreResponse, error) {
	_ = ctx
	_ = req
	return &runtimev1.RestoreResponse{
		Code:    1,
		Message: "restore is not supported by runtime-launcher SandboxService backend",
	}, nil
}

func (s *LauncherService) startWithConfig(
	ctx context.Context,
	cfg *runtime.CreateConfig,
) (*runtimev1.StartResponse, error) {
	containerID, err := s.runtime.Create(ctx, cfg)
	if err != nil {
		log.Printf("[service] Start failed: %v", err)
		return &runtimev1.StartResponse{Code: 1, Message: fmt.Sprintf("create sandbox failed: %v", err)}, nil
	}

	var endpoint *runtime.NetworkEndpoint
	if networkHasIsolatedEndpoint(cfg.Network) {
		endpoint, err = s.runtime.ResolveEndpoint(ctx, containerID)
		if err != nil {
			log.Printf("[service] resolve endpoint for sandbox %s failed: %v", containerID, err)
			s.cleanupFailedStart(containerID)
			return &runtimev1.StartResponse{
				Code:    1,
				Message: fmt.Sprintf("resolve sandbox endpoint failed: %v", err),
			}, nil
		}
		if endpoint == nil {
			log.Printf("[service] runtime returned no endpoint for sandbox %s", containerID)
			s.cleanupFailedStart(containerID)
			return &runtimev1.StartResponse{Code: 1, Message: "runtime returned no sandbox endpoint"}, nil
		}
		if endpoint.SandboxIP == "" {
			log.Printf(
				"[service] invalid endpoint for sandbox %s: sandbox_ip=%q",
				containerID,
				endpoint.SandboxIP,
			)
			s.cleanupFailedStart(containerID)
			return &runtimev1.StartResponse{Code: 1, Message: "runtime returned an invalid sandbox endpoint"}, nil
		}
	}
	runtimeID := cfg.ID
	if runtimeID == "" {
		runtimeID = containerID
	}
	s.stateMgr.AddContainer(containerID, runtimeID, cfg)
	go s.watchContainer(containerID)
	response := &runtimev1.StartResponse{Code: 0, Id: containerID}
	if endpoint != nil {
		response.SandboxIp = endpoint.SandboxIP
	}
	return response, nil
}

func (s *LauncherService) cleanupFailedStart(containerID string) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), endpointCleanupTimeout)
	defer cancel()
	if err := s.runtime.Delete(cleanupCtx, containerID, 0); err != nil {
		log.Printf("[service] cleanup sandbox %s after endpoint failure failed: %v", containerID, err)
	}
}

func networkHasIsolatedEndpoint(network string) bool {
	mode := strings.ToLower(strings.TrimSpace(network))
	return mode != "" && mode != "host" && mode != "none" && !strings.HasPrefix(mode, "container:")
}

// Delete stops the sandbox container and removes its tracked state.
func (s *LauncherService) Delete(ctx context.Context, req *runtimev1.DeleteRequest) (*runtimev1.DeleteResponse, error) {
	containerID := req.GetId()
	if containerID == "" {
		return &runtimev1.DeleteResponse{}, fmt.Errorf("sandbox id cannot be empty")
	}
	log.Printf("[service] Delete: id=%s timeout=%d", containerID, req.GetTimeout())
	if err := s.runtime.Delete(ctx, containerID, req.GetTimeout()); err != nil {
		log.Printf("[service] Delete failed: %v", err)
	}
	s.stateMgr.RemoveContainer(containerID)
	return &runtimev1.DeleteResponse{}, nil
}

// Wait waits until the sandbox container exits.
func (s *LauncherService) Wait(ctx context.Context, req *runtimev1.WaitRequest) (*runtimev1.WaitResponse, error) {
	containerID := req.GetId()
	if containerID == "" {
		return &runtimev1.WaitResponse{Status: 1, Message: "sandbox id cannot be empty"}, nil
	}
	cs, ok := s.stateMgr.GetContainer(containerID)
	if !ok {
		status, err := s.runtime.Wait(ctx, containerID)
		if err != nil {
			return &runtimev1.WaitResponse{Status: 1, Message: err.Error()}, nil
		}
		return &runtimev1.WaitResponse{Status: status.StatusCode, ExitCode: status.ExitCode, Message: status.Message}, nil
	}
	select {
	case <-cs.DoneCh:
		if latest, ok := s.stateMgr.GetContainer(containerID); ok {
			return &runtimev1.WaitResponse{Status: 0, ExitCode: latest.ExitCode, Message: latest.ExitMessage}, nil
		}
		return &runtimev1.WaitResponse{Status: 0, ExitCode: cs.ExitCode, Message: cs.ExitMessage}, nil
	case <-ctx.Done():
		return &runtimev1.WaitResponse{Status: 1, Message: "wait timeout or canceled"}, nil
	}
}

// List returns sandbox containers known to the backend and matching the selector.
func (s *LauncherService) List(
	ctx context.Context,
	req *runtimev1.ListSandboxesRequest,
) (*runtimev1.ListSandboxesResponse, error) {
	infos, err := s.runtime.List(ctx, req.GetId())
	if err != nil {
		log.Printf("[service] List: err=%v", err)
		return nil, status.Errorf(codes.Internal, "list sandboxes failed: %v", err)
	}
	statuses := make([]*runtimev1.SandboxStatus, 0, len(infos))
	for _, info := range infos {
		status := containerInfoToSandboxStatus(info, s.stateMgr)
		if !matchesSelector(status.GetLabels(), req.GetSelector()) {
			continue
		}
		statuses = append(statuses, status)
	}
	return &runtimev1.ListSandboxesResponse{Sandboxes: statuses}, nil
}

// Stats returns resource usage for a sandbox container.
func (s *LauncherService) Stats(ctx context.Context, req *runtimev1.StatsRequest) (*runtimev1.StatsResponse, error) {
	id := req.GetId()
	cs, err := s.runtime.Stats(ctx, id)
	if err != nil {
		log.Printf("[service] Stats: id=%s err=%v", id, err)
		return &runtimev1.StatsResponse{}, nil
	}
	return &runtimev1.StatsResponse{
		CpuUsageNs:          cs.CPUUsageNs,
		MemoryUsageBytes:    cs.MemoryUsageBytes,
		MemoryLimitBytes:    cs.MemoryLimitBytes,
		MemoryMaxUsageBytes: cs.MemoryMaxUsageBytes,
	}, nil
}

// Register stores reusable sandbox templates for later Start requests.
func (s *LauncherService) Register(
	ctx context.Context,
	req *runtimev1.RegisterRequest,
) (*runtimev1.NormalResponse, error) {
	_ = ctx
	for _, tmpl := range req.GetTemplates() {
		s.stateMgr.RegisterTemplate(tmpl)
		log.Printf(
			"[service] registered template: id=%s runtime=%s makeSeed=%v",
			tmpl.GetId(),
			tmpl.GetRuntime(),
			tmpl.GetMakeSeed(),
		)
	}
	return &runtimev1.NormalResponse{Success: true, Message: "registered"}, nil
}

// Unregister removes reusable sandbox templates.
func (s *LauncherService) Unregister(
	ctx context.Context,
	req *runtimev1.UnregisterRequest,
) (*runtimev1.NormalResponse, error) {
	_ = ctx
	for _, id := range req.GetIds() {
		s.stateMgr.UnregisterTemplate(id)
	}
	return &runtimev1.NormalResponse{Success: true, Message: "unregistered"}, nil
}

// GetRegistered lists reusable sandbox templates stored in memory.
func (s *LauncherService) GetRegistered(
	ctx context.Context,
	req *runtimev1.GetRegisteredRequest,
) (*runtimev1.GetRegisteredResponse, error) {
	_ = ctx
	_ = req
	return &runtimev1.GetRegisteredResponse{Templates: s.stateMgr.GetAllRegisteredTemplates()}, nil
}

// Checkpoint reports that runtime-launcher does not implement checkpointing.
func (s *LauncherService) Checkpoint(
	ctx context.Context,
	req *runtimev1.CheckpointRequest,
) (*runtimev1.CheckpointResponse, error) {
	_ = ctx
	_ = req
	return nil, status.Error(codes.Unimplemented,
		"checkpoint is not supported by runtime-launcher SandboxService backend")
}

// ListAvailableRuntimes returns an empty successful snapshot because
// runtime-launcher does not expose sandboxd runtime-class handlers.
func (s *LauncherService) ListAvailableRuntimes(
	ctx context.Context,
	req *runtimev1.ListAvailableRuntimesRequest,
) (*runtimev1.ListAvailableRuntimesResponse, error) {
	_ = ctx
	_ = req
	return &runtimev1.ListAvailableRuntimesResponse{}, nil
}

func (s *LauncherService) buildCreateConfig(req *runtimev1.StartRequest) *runtime.CreateConfig {
	cfg := buildCreateConfig(req)
	templateID := strings.TrimSpace(req.GetTemplateId())
	if templateID == "" {
		return cfg
	}
	tmpl, ok := s.stateMgr.GetRegisteredTemplate(templateID)
	if !ok {
		return cfg
	}
	cfg.Envs = mergeEnvMaps(tmpl.GetEnvs(), cfg.Envs)
	return cfg
}

func buildCreateConfig(req *runtimev1.StartRequest) *runtime.CreateConfig {
	id := req.GetSandboxId()
	if id == "" {
		id = req.GetEnvs()["YR_RUNTIME_ID"]
	}
	rootfs := convertRootfs(req.GetRootfs())
	sandbox := req.GetRuntime()
	if rootfs.Type == runtime.RootfsSrcImage && rootfs.ImageURL != "" {
		sandbox = rootfs.ImageURL
	}
	cpuMilli := defaultCPUMillicore
	memMB := defaultMemoryMB
	if v, ok := req.GetResources()["CPU"]; ok {
		cpuMilli = v
	}
	if v, ok := req.GetResources()["Memory"]; ok {
		memMB = v
	}
	network := normalizeNetwork(req.GetNetwork())
	return &runtime.CreateConfig{
		ID:           id,
		Sandbox:      sandbox,
		Rootfs:       rootfs,
		Command:      req.GetCommand(),
		Envs:         cloneMap(req.GetEnvs()),
		Mounts:       convertProtoMounts(req.GetMounts()),
		CPUMillicore: cpuMilli,
		MemoryMB:     memMB,
		Stdout:       req.GetStdout(),
		Stderr:       req.GetStderr(),
		ExtraConfig:  req.GetExtraConfig(),
		Network:      network,
		Ports:        req.GetPorts(),
		Labels:       cloneMap(req.GetLabels()),
	}
}

func normalizeNetwork(network string) string {
	network = strings.TrimSpace(network)
	if network == "" || network == "sandbox" {
		return "bridge"
	}
	if strings.HasPrefix(network, "{") {
		var payload map[string]any
		if err := json.Unmarshal([]byte(network), &payload); err == nil {
			modeValue, modeExists := payload["mode"]
			if mode, ok := modeValue.(string); modeExists && ok && strings.TrimSpace(mode) != "" {
				return strings.TrimSpace(mode)
			}
			// sandboxd callers pass portForwardings as JSON in network while
			// the actual publish rules are carried in StartRequest.ports.
			// Docker expects NetworkMode to be a real mode/name, not the JSON blob.
			return "bridge"
		}
		return "bridge"
	}
	return network
}

func convertRootfs(protoRootfs *runtimev1.RootfsConfig) runtime.RootfsConfig {
	if protoRootfs == nil {
		return runtime.RootfsConfig{}
	}
	rootfs := runtime.RootfsConfig{Readonly: protoRootfs.GetReadonly(), Type: runtime.RootfsSrcType(protoRootfs.GetType())}
	rootfs.ImageURL = protoRootfs.GetImageUrl()
	rootfs.S3 = convertS3(protoRootfs.GetS3Config())
	return rootfs
}

func convertS3(s3 *runtimev1.S3Config) *runtime.S3Config {
	if s3 == nil {
		return nil
	}
	return &runtime.S3Config{
		Endpoint:        s3.GetEndpoint(),
		Bucket:          s3.GetBucket(),
		Object:          s3.GetObject(),
		AccessKeyID:     s3.GetAccessKeyId(),
		AccessKeySecret: s3.GetAccessKeySecret(),
	}
}

func cloneMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func mergeEnvMaps(base map[string]string, overrides map[string]string) map[string]string {
	out := cloneMap(base)
	for k, v := range overrides {
		out[k] = v
	}
	return out
}

func convertProtoMounts(protoMounts []*runtimev1.Mount) []runtime.MountConfig {
	mounts := make([]runtime.MountConfig, 0, len(protoMounts))
	for _, m := range protoMounts {
		mounts = append(mounts, runtime.MountConfig{
			Type:     m.GetType(),
			Target:   m.GetTarget(),
			Options:  m.GetOptions(),
			HostPath: m.GetHostPath(),
			S3:       convertS3(m.GetS3Config()),
			ImageURL: m.GetImageUrl(),
		})
	}
	return mounts
}

func containerInfoToSandboxStatus(info *runtime.ContainerInfo, stateMgr *state.Manager) *runtimev1.SandboxStatus {
	labels := sandboxLabels(info)
	if cs, ok := stateMgr.GetContainer(info.ID); ok {
		return containerStateToSandboxStatus(cs, labels)
	}
	status := runtimev1.SandboxState_SANDBOX_STATE_UNKNOWN
	switch info.State {
	case "running":
		status = runtimev1.SandboxState_SANDBOX_STATE_RUNNING
	case "exited", "dead":
		status = runtimev1.SandboxState_SANDBOX_STATE_EXITED
	}
	return &runtimev1.SandboxStatus{
		Id:         info.ID,
		Command:    append([]string(nil), info.Command...),
		Runtime:    info.Image,
		State:      status,
		StartedAt:  info.StartedAt,
		FinishedAt: info.FinishedAt,
		ExitCode:   info.ExitCode,
		Message:    info.Message,
		Labels:     labels,
	}
}

func sandboxLabels(info *runtime.ContainerInfo) map[string]string {
	labels := make(map[string]string, len(info.Labels)+runtimeLabelSlots)
	for k, v := range info.Labels {
		labels[k] = v
	}
	if info.RuntimeID != "" {
		labels[runtime.RuntimeIDLabelKey] = info.RuntimeID
		// Keep the historical helper label for humans/scripts while selectors can
		// also use the backend-authoritative yr.runtime-id label.
		labels["runtime_id"] = info.RuntimeID
	}
	return labels
}

func containerStateToSandboxStatus(cs *state.ContainerState, labels map[string]string) *runtimev1.SandboxStatus {
	status := runtimev1.SandboxState_SANDBOX_STATE_RUNNING
	if cs.Exited {
		status = runtimev1.SandboxState_SANDBOX_STATE_EXITED
	}
	cfg := cs.Config
	sandboxStatus := &runtimev1.SandboxStatus{
		Id:         cs.ID,
		State:      status,
		StartedAt:  cs.StartedAt,
		FinishedAt: cs.FinishedAt,
		ExitCode:   cs.ExitCode,
		Message:    cs.ExitMessage,
		Labels:     labels,
	}
	if cfg != nil {
		sandboxStatus.Command = append([]string(nil), cfg.Command...)
		sandboxStatus.Runtime = cfg.Sandbox
		sandboxStatus.Mounts = runtimeMountsToProto(cfg.Mounts)
		sandboxStatus.Envs = envMapToKeyValues(cfg.Envs)
		sandboxStatus.Stdout = cfg.Stdout
		sandboxStatus.Stderr = cfg.Stderr
		sandboxStatus.Resources = &runtimev1.LinuxSandboxResources{
			MemoryLimitInBytes: int64(cfg.MemoryMB * 1024 * 1024),
		}
		if cfg.CPUMillicore > 0 {
			sandboxStatus.Resources.CpuPeriod = 100000
			sandboxStatus.Resources.CpuQuota = int64(cfg.CPUMillicore * cpuQuotaMultiplier)
		}
	}
	return sandboxStatus
}

func runtimeMountsToProto(mounts []runtime.MountConfig) []*runtimev1.Mount {
	result := make([]*runtimev1.Mount, 0, len(mounts))
	for _, m := range mounts {
		pm := &runtimev1.Mount{Type: m.Type, Target: m.Target, Options: append([]string(nil), m.Options...)}
		if m.HostPath != "" {
			pm.Source = &runtimev1.Mount_HostPath{HostPath: m.HostPath}
		} else if m.ImageURL != "" {
			pm.Source = &runtimev1.Mount_ImageUrl{ImageUrl: m.ImageURL}
		} else if m.S3 != nil {
			pm.Source = &runtimev1.Mount_S3Config{S3Config: &runtimev1.S3Config{
				Endpoint:        m.S3.Endpoint,
				Bucket:          m.S3.Bucket,
				Object:          m.S3.Object,
				AccessKeyId:     m.S3.AccessKeyID,
				AccessKeySecret: m.S3.AccessKeySecret,
			}}
		}
		result = append(result, pm)
	}
	return result
}

func envMapToKeyValues(envs map[string]string) []*runtimev1.KeyValue {
	result := make([]*runtimev1.KeyValue, 0, len(envs))
	for k, v := range envs {
		result = append(result, &runtimev1.KeyValue{Key: k, Value: v})
	}
	return result
}

func matchesSelector(labels, selector map[string]string) bool {
	for k, v := range selector {
		if labels[k] != v {
			return false
		}
	}
	return true
}

func (s *LauncherService) watchContainer(containerID string) {
	status, err := s.runtime.Wait(context.Background(), containerID)
	if err != nil {
		log.Printf("[service] watch sandbox %s failed: %v", containerID, err)
		s.stateMgr.MarkExited(containerID, 1, err.Error())
		return
	}
	s.stateMgr.MarkExited(containerID, status.ExitCode, status.Message)
}
