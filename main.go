package main

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

// 模拟数据库模型
type FBSDBModel struct {
	PrintAny          interface{}
	TASK_TYPE_BACKUP  string
	TASK_TYPE_RESTORE string
}

// 模拟磁带驱动器模型
type FBSResTapeDriver struct {
	StFile string
}

// 模拟备份资源
type TapeBakResourceTemp struct {
	Resource []string
}

// 客户端结构
type Client struct {
	clientId string
}

// 模拟备份任务
type FBSBackupTask struct {
	taskName string
	runId    string
	device   string
	clients  []Client // 任务中的客户端列表
}

func (t *FBSBackupTask) getBakTaskTapeDetail(poolId string) (TapeBakResourceTemp, error) {
	// 模拟获取磁带详情
	return TapeBakResourceTemp{
		Resource: []string{"tape1", "tape2", "tape3"},
	}, nil
}

func (t *FBSBackupTask) Cancel() {
	// 模拟取消任务
	log.Printf("备份任务 %s 已取消", t.taskName)
}

// 模拟恢复任务
type FBSRestoreTask struct {
	taskName string
	runId    string
	device   string
	clients  []Client // 任务中的客户端列表
}

func (t *FBSRestoreTask) getRestoreTaskTapeDetail(poolId string) (TapeBakResourceTemp, error) {
	// 模拟获取恢复所需的磁带详情
	return TapeBakResourceTemp{
		Resource: []string{"tape1", "tape4", "tape5"},
	}, nil
}

func (t *FBSRestoreTask) Cancel() {
	// 模拟取消任务
	log.Printf("恢复任务 %s 已取消", t.taskName)
}

// 任务执行状态
type TaskExecution struct {
	taskType      string
	clients       []Client
	currentClient int
	driver        string
	wg            sync.WaitGroup
	mu            sync.Mutex
}

// 模拟ORM
type TaskOrm struct{}

func (o *TaskOrm) GetTapeoolDriver(poolId string) ([]FBSResTapeDriver, error) {
	// 模拟从数据库获取驱动器
	return []FBSResTapeDriver{
		{StFile: "/dev/tape0"},
		{StFile: "/dev/tape1"},
		{StFile: "/dev/tape2"},
	}, nil
}

var FBSOrm = struct {
	GetTaskOrm func() *TaskOrm
}{
	GetTaskOrm: func() *TaskOrm { return &TaskOrm{} },
}

// TapeStoragePool 磁带存储池信息
type TapeStoragePool struct {
	storageId        string          // 存储池Id
	drivers          []string        // 存储池的驱动器
	tapes            []string        // 存储池的磁带
	allocatedDrivers map[string]bool // 已分配但未释放的驱动器
	mu               sync.Mutex      // 保护存储池资源的互斥锁
	cond             *sync.Cond      // 条件变量，用于等待资源释放
}

// TapeDriverMgr 驱动器管理器
type TapeDriverMgr struct {
	mu           sync.Mutex                  // 保护存储池的互斥锁
	storagePools map[string]*TapeStoragePool // 存储池资源管理
	taskExecs    map[string]*TaskExecution   // 任务执行状态
}

// 全局变量
var (
	onceTape            sync.Once
	GlobalTapeDriverMgr *TapeDriverMgr
)

// NewTapeDriverMgr 初始化驱动器管理器
func NewTapeDriverMgr() *TapeDriverMgr {
	onceTape.Do(func() {
		GlobalTapeDriverMgr = &TapeDriverMgr{
			storagePools: make(map[string]*TapeStoragePool),
			taskExecs:    make(map[string]*TaskExecution),
		}
	})
	return GlobalTapeDriverMgr
}

// getPool 获取存储池
func (mgr *TapeDriverMgr) getPool(poolId string) *TapeStoragePool {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	pool, exists := mgr.storagePools[poolId]
	if !exists {
		pool = &TapeStoragePool{
			storageId:        poolId,
			allocatedDrivers: make(map[string]bool),
		}
		pool.cond = sync.NewCond(&pool.mu)
		mgr.storagePools[poolId] = pool
	}
	return pool
}

// ReleaseDriver 释放驱动器
func (mgr *TapeDriverMgr) ReleaseDriver(poolId, driver string, taskName string) {
	pool := mgr.getPool(poolId)

	pool.mu.Lock()
	defer pool.mu.Unlock()

	if allocated, exists := pool.allocatedDrivers[driver]; exists && allocated {
		pool.allocatedDrivers[driver] = false
		pool.cond.Broadcast() // 通知所有等待的任务
		log.Printf("驱动器 %s 已释放，任务: %s", driver, taskName)
	}

	// 从任务执行状态中移除
	mgr.mu.Lock()
	delete(mgr.taskExecs, taskName)
	mgr.mu.Unlock()
}

// AllocateDriver 通用驱动器分配函数
func (mgr *TapeDriverMgr) AllocateDriver(poolId string, taskName string, clients []Client, getTapeDetail func(string) (TapeBakResourceTemp, error)) (string, error) {
	pool := mgr.getPool(poolId)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	orm := FBSOrm.GetTaskOrm()
	if orm == nil {
		return "", errors.New("task orm 未初始化")
	}

	// 获取存储池对应带库可使用的驱动器信息
	drivers, err := orm.GetTapeoolDriver(poolId)
	if err != nil {
		return "", err
	}

	// 判断可用的驱动器数量
	if len(drivers) == 0 {
		return "", errors.New("没有可用的驱动器")
	}

	// 获取任务所需的磁带信息
	if getTapeDetail != nil {
		bakResource, err := getTapeDetail(poolId)
		if err != nil {
			return "", err
		}

		if len(bakResource.Resource) == 0 {
			return "", errors.New("没有可用的磁带")
		}
	}

	// 填充驱动器信息
	pool.mu.Lock()
	if len(pool.drivers) == 0 {
		pool.drivers = make([]string, 0, len(drivers))
		pool.allocatedDrivers = make(map[string]bool)
		for _, driver := range drivers {
			driverPath := driver.StFile
			pool.drivers = append(pool.drivers, driverPath)
			pool.allocatedDrivers[driverPath] = false
		}
	}
	pool.mu.Unlock()

	// 检查任务是否已经分配了驱动器
	mgr.mu.Lock()
	if taskExec, exists := mgr.taskExecs[taskName]; exists {
		mgr.mu.Unlock()
		return taskExec.driver, nil
	}
	mgr.mu.Unlock()

	// 使用带有超时的等待循环
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
			pool.mu.Lock()
			// 查找可用驱动器
			for _, driver := range pool.drivers {
				if !pool.allocatedDrivers[driver] {
					pool.allocatedDrivers[driver] = true
					pool.mu.Unlock()

					// 记录任务执行状态
					mgr.mu.Lock()
					mgr.taskExecs[taskName] = &TaskExecution{
						clients:       clients,
						currentClient: 0,
						driver:        driver,
					}
					mgr.mu.Unlock()

					return driver, nil
				}
			}

			// 没有可用驱动器，等待
			pool.cond.Wait()
			pool.mu.Unlock()
		}
	}
}

// AllocateBakDriver 分配备份驱动器
func (mgr *TapeDriverMgr) AllocateBakDriver(poolId string, obj *FBSBackupTask) (string, error) {
	return mgr.AllocateDriver(poolId, obj.taskName, obj.clients, obj.getBakTaskTapeDetail)
}

// AllocateRestoreDriver 分配恢复驱动器
func (mgr *TapeDriverMgr) AllocateRestoreDriver(poolId string, obj *FBSRestoreTask) (string, error) {
	return mgr.AllocateDriver(poolId, obj.taskName, obj.clients, obj.getRestoreTaskTapeDetail)
}

// ExecuteNextClient 执行任务的下一个客户端
func (mgr *TapeDriverMgr) ExecuteNextClient(poolId, taskName string) bool {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	taskExec, exists := mgr.taskExecs[taskName]
	if !exists {
		log.Printf("任务 %s 不存在", taskName)
		return false
	}

	// 检查是否所有客户端都已执行
	if taskExec.currentClient >= len(taskExec.clients) {
		log.Printf("任务 %s 的所有客户端已执行完毕", taskName)
		return false
	}

	// 获取当前要执行的客户端
	client := taskExec.clients[taskExec.currentClient]
	log.Printf("开始执行任务 %s 的客户端 %s，使用驱动器 %s", taskName, client.clientId, taskExec.driver)

	// 执行客户端任务（这里只是模拟）
	// 实际应用中，这里会调用具体的客户端处理逻辑

	// 标记当前客户端已执行
	taskExec.currentClient++

	// 检查是否还有下一个客户端
	if taskExec.currentClient < len(taskExec.clients) {
		log.Printf("任务 %s 还有 %d 个客户端等待执行", taskName, len(taskExec.clients)-taskExec.currentClient)
		return true
	}

	return false
}

// 测试函数
func testAllocateDrivers() {
	mgr := NewTapeDriverMgr()
	poolId := "pool1"

	// 模拟多个并发任务（混合备份和恢复任务）
	var wg sync.WaitGroup

	// 创建备份任务，包含2个客户端
	backupTask := &FBSBackupTask{
		taskName: "backup-task1",
		runId:    "run1",
		clients: []Client{
			{clientId: "client1"},
			{clientId: "client2"},
		},
	}

	// 创建恢复任务，包含2个客户端
	restoreTask := &FBSRestoreTask{
		taskName: "restore-task1",
		runId:    "run100",
		clients: []Client{
			{clientId: "clientA"},
			{clientId: "clientB"},
		},
	}

	// 启动备份任务处理
	wg.Add(1)
	go func() {
		defer wg.Done()

		// 分配驱动器
		device, err := mgr.AllocateBakDriver(poolId, backupTask)
		if err != nil {
			log.Printf("备份任务分配驱动器失败: %v", err)
			return
		}

		log.Printf("备份任务分配到驱动器: %s", device)

		// 顺序执行所有客户端
		for {
			hasNext := mgr.ExecuteNextClient(poolId, backupTask.taskName)
			if !hasNext {
				break
			}

			// 模拟客户端执行时间
			time.Sleep(time.Second * 2)
		}

		// 释放驱动器
		mgr.ReleaseDriver(poolId, device, backupTask.taskName)
		log.Printf("备份任务已完成")
	}()

	// 启动恢复任务处理
	wg.Add(1)
	go func() {
		defer wg.Done()

		// 分配驱动器
		device, err := mgr.AllocateRestoreDriver(poolId, restoreTask)
		if err != nil {
			log.Printf("恢复任务分配驱动器失败: %v", err)
			return
		}

		log.Printf("恢复任务分配到驱动器: %s", device)

		// 顺序执行所有客户端
		for {
			hasNext := mgr.ExecuteNextClient(poolId, restoreTask.taskName)
			if !hasNext {
				break
			}

			// 模拟客户端执行时间
			time.Sleep(time.Second * 3)
		}

		// 释放驱动器
		mgr.ReleaseDriver(poolId, device, restoreTask.taskName)
		log.Printf("恢复任务已完成")
	}()

	wg.Wait()
}

func main() {
	log.Println("开始测试磁带驱动器分配...")
	testAllocateDrivers()
	log.Println("测试完成")
}
