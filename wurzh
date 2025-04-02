import random
import numpy as np

# 定义任务类，包含任务ID、资源需求、截止时间和处理时长
class Task:
    def __init__(self, task_id, required, deadline, processing_time):
        self.task_id = task_id
        self.required = required
        self.deadline = deadline
        self.processing_time = processing_time
        self.assigned_node = None
        self.start_time = None
        self.end_time = None

# 定义节点类，每个节点有固定的资源容量和当前负载
class Node:
    def __init__(self, node_id, capacity):
        self.node_id = node_id
        self.capacity = capacity
        self.current_load = 0
        self.schedule = []  # 存放当前调度到该节点的任务

    def available_capacity(self):
        return self.capacity - self.current_load

# 调度函数：基于启发式规则对待调度任务分配节点
def schedule_tasks(tasks, nodes, current_time, meta_params):
    # meta_params 中的参数影响调度策略，这里用 alpha 和 beta 权衡可用容量和当前负载
    alpha, beta = meta_params['alpha'], meta_params['beta']
    # 按截止时间排序，先调度截止时间早的任务
    tasks_sorted = sorted(tasks, key=lambda t: t.deadline)
    for task in tasks_sorted:
        # 找出能满足任务资源需求的节点
        candidate_nodes = [node for node in nodes if node.available_capacity() >= task.required]
        if not candidate_nodes:
            # 当前没有可用节点，任务将延后调度
            continue
        # 启发式得分：希望选择剩余资源多且当前负载低的节点
        candidate_nodes.sort(key=lambda n: alpha * n.available_capacity() - beta * n.current_load, reverse=True)
        chosen = candidate_nodes[0]
        # 分配任务
        task.assigned_node = chosen.node_id
        task.start_time = current_time
        task.end_time = current_time + task.processing_time
        chosen.schedule.append(task)
        # 假设任务一旦调度，其所需资源在处理期间被占用
        chosen.current_load += task.required
    return tasks

# 模拟时间推进：在每个时间步释放已经完成的任务占用的资源
def update_nodes(nodes, current_time):
    for node in nodes:
        finished_tasks = []
        for task in node.schedule:
            if task.end_time <= current_time:
                finished_tasks.append(task)
        for task in finished_tasks:
            node.schedule.remove(task)
            node.current_load -= task.required

# 主仿真函数：初始化节点和任务，并在时间步中不断调度任务
def simulate_scheduling(num_nodes=3, num_tasks=10, simulation_time=20):
    # 随机生成各节点容量
    nodes = [Node(node_id=i, capacity=random.randint(50, 100)) for i in range(num_nodes)]
    tasks = []
    # 随机生成任务（资源需求、截止时间、处理时长）
    for i in range(num_tasks):
        required = random.randint(10, 30)
        deadline = random.randint(5, simulation_time)  # 截止时间在仿真时间内
        processing_time = random.randint(1, 5)
        tasks.append(Task(task_id=i, required=required, deadline=deadline, processing_time=processing_time))
    tasks_queue = tasks.copy()
    current_time = 0
    # 初始元参数（这相当于元网络生成的参数）
    meta_params = {'alpha': 1.0, 'beta': 0.5}

    print("仿真开始...\n")
    while current_time < simulation_time:
        print(f"=== 时间步：{current_time} ===")
        # 释放完成任务占用的资源
        update_nodes(nodes, current_time)
        # 选择待调度任务：未分配且截止时间未过的任务
        pending_tasks = [task for task in tasks_queue if task.start_time is None and task.deadline >= current_time]
        if pending_tasks:
            scheduled = schedule_tasks(pending_tasks, nodes, current_time, meta_params)
            for task in scheduled:
                if task.assigned_node is not None:
                    print(f"任务 {task.task_id} 分配到节点 {task.assigned_node}，开始时间 {task.start_time}，结束时间 {task.end_time}，截止时间 {task.deadline}")
        else:
            print("当前无待调度任务。")
        # 根据各节点平均负载更新元参数（这里简单地认为负载高时需更严格地惩罚高负载）
        avg_load = np.mean([node.current_load for node in nodes])
        meta_params['beta'] = 0.5 + 0.1 * (avg_load / 50)
        print(f"更新元参数：{meta_params}")
        current_time += 1
        print()
    
    print("仿真结束。最终各节点状态：")
    for node in nodes:
        print(f"节点 {node.node_id}: 当前负载 {node.current_load}, 正在调度的任务 {[task.task_id for task in node.schedule]}")

if __name__ == "__main__":
    simulate_scheduling()
