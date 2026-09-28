# yr CLI 信号修复版本构建与测试配方(实跑脚本逐字入仓)

## 背景(只读证据,不称历史故障定案)

- 镜像内实际 yr(`/home/yuanrong/functionsystem/bin/yr`,SHA
  `77b4bfa8…`,go1.24.1/CGO=0/trimpath,无 VCS)符号含
  **cli/internal/start.blockRun**(6ff3975 修复前的旧路径:
  blockRun 内 signal-handling + os.Exit);**prepareBlockedCommand 不存在**
  (可能内联,单独不足定案;两者并存与"旧版"一致)。
- canonical 6ff3975《fix(cli): wait for graceful deployment shutdown》
  (df2e2f2a 祖先)改为 prepareBlockedCommand(Pdeathsig SIGTERM)+
  ExecCommandUntil 接管信号转发与等待,并有真实子进程测试
  cmd_args_shutdown_test.go。
- **本镜像线只替换过四个 C++ 程序目录,yr ELF 从未随产物更新**——
  修复存在于源码但未随任何候选镜像部署,是与滚动窗口陈旧端点
  直接相关的已知缺口。

## 实跑结果(各步独立 rc,BM1 /data/work/yr-cli-verify)

- 源:df2e2f2a `git archive -- functionsystem/apps/cli`
  (tar SHA `3d355ad5…`,63 受控文件,树与归档逐字节一致;
  go.mod/go.sum 前后 SHA 未变)
- 信号测试(真实 SIGTERM/SIGINT 子进程场景):
  **4/4 PASS rc=0**(TestBlockedCommandWaitsForGracefulShutdown /
  TestBlockedCommandWaitsForShutdownAfterReady 及子场景);
  utils+start vet rc=0
- protobuf:proto/message.proto(SHA `67d5b61b…`)经
  protoc + protoc-gen-go@v1.36.6(`5bdd8ca0…`)+
  protoc-gen-go-grpc@v1.5.1(`79811d7a…`)生成 internal/pb
  (message.pb.go SHA `d4fa9c39…`);a1 缺 pb 失败原件保留
- 构建(go1.24.1、CGO_ENABLED=0、-trimpath、-p 12、-mod=readonly):
  **rc=0,产物 yr 23,457,955 字节,SHA
  `ecff370c8e6fdd4f34260a331366e8bc8c1635a8b557082dff5ea5dade54387e`**
- 新二进制符号验证:**blockRun=0、prepareBlockedCommand=1、
  ExecCommandUntil=10、Pdeathsig=1**(与修复后源码一致)

## 脚本(逐字同实跑,SHA 见上)

- `build-yr-cli.sh`(5959c6a5…)
- `run-cli-signal-tests.sh`(3d953245…)

## 边界

- 未构建/部署含此 yr 的新镜像;未跑物理例;
- 旧 Pod 退出日志不可得,旧 yr 实际退出序列为**未知**(符号证据
  仅支持"旧路径存在",不回写历史实测)。
