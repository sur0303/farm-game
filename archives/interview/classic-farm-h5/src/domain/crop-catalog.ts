export interface CropDefinition {
  id: number;
  name: string;
  icon: string;
  seedItemId: number;
  produceItemId: number;
  offerId: number;
  buyPrice: number;
  sellPrice: number;
  offerVersion: number;
  matureAfterMs: number;
}

// These IDs mirror farm-game/internal/cropcatalog. They are part of the
// persisted protocol, so adding a crop means adding a new ID rather than
// reusing an old one.
export const CROP_CATALOG: readonly CropDefinition[] = [
  { id: 1, name: '胡萝卜', icon: '🥕', seedItemId: 1001, produceItemId: 2001, offerId: 1, buyPrice: 10, offerVersion: 1, sellPrice: 15, matureAfterMs: 10_000 },
  { id: 2, name: '番茄', icon: '🍅', seedItemId: 1002, produceItemId: 2002, offerId: 2, buyPrice: 15, offerVersion: 1, sellPrice: 22, matureAfterMs: 15_000 },
  { id: 3, name: '玉米', icon: '🌽', seedItemId: 1003, produceItemId: 2003, offerId: 3, buyPrice: 20, offerVersion: 1, sellPrice: 30, matureAfterMs: 20_000 },
];

export const cropById = (id: number): CropDefinition =>
  CROP_CATALOG.find((crop) => crop.id === id) ?? CROP_CATALOG[0];

export const cropByOfferId = (offerId: number): CropDefinition | undefined =>
  CROP_CATALOG.find((crop) => crop.offerId === offerId);

export const cropBySeedItemId = (itemId: number): CropDefinition | undefined =>
  CROP_CATALOG.find((crop) => crop.seedItemId === itemId);

export const cropByProduceItemId = (itemId: number): CropDefinition | undefined =>
  CROP_CATALOG.find((crop) => crop.produceItemId === itemId);

export const inventoryKeyForItem = (itemId: number): string => {
  const crop = cropBySeedItemId(itemId);
  if (crop) return crop.id === 1 ? 'seed' : `${crop.name}种子`;
  const produce = cropByProduceItemId(itemId);
  if (produce) return produce.id === 1 ? 'carrot' : produce.name;
  return String(itemId);
};

export const seedInventoryKey = (crop: CropDefinition): string => crop.id === 1 ? 'seed' : `${crop.name}种子`;
export const produceInventoryKey = (crop: CropDefinition): string => crop.id === 1 ? 'carrot' : crop.name;
