export interface TaskDefinition {
  id: number;
  name: string;
  description: string;
  rewardCoin: number;
  rewardExp: number;
}

export const TASK_CATALOG: readonly TaskDefinition[] = [
  { id: 1, name: '新手收获', description: '收获任意 3 个成熟作物', rewardCoin: 30, rewardExp: 10 },
  { id: 2, name: '备足种子', description: '在商店购买 3 份种子', rewardCoin: 20, rewardExp: 5 },
];

export const taskById = (id: number): TaskDefinition | undefined => TASK_CATALOG.find((task) => task.id === id);
