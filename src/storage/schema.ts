import type { CollectionSchema, IndexDef } from '../core/types.js';

export interface StoreIndex {
  name: string;
  keyPath: string | string[];
  unique?: boolean;
}

export interface StoreDefinition {
  name: string;
  keyPath: string;
  autoIncrement: boolean;
  indexes: StoreIndex[];
}

export function buildStoreDefinition(
  collectionName: string,
  schema: CollectionSchema,
): StoreDefinition {
  const indexes: StoreIndex[] = [
    { name: '_updatedAt', keyPath: '_updatedAt' },
    { name: '_deleted', keyPath: '_deleted' },
  ];

  for (const indexDef of schema.indexes ?? []) {
    const index = normalizeIndex(indexDef);
    if (!indexes.some((i) => i.name === index.name)) {
      indexes.push(index);
    }
  }

  return {
    name: collectionName,
    keyPath: '_id',
    autoIncrement: false,
    indexes,
  };
}

function normalizeIndex(def: IndexDef): StoreIndex {
  if (typeof def === 'string') {
    return { name: def, keyPath: def };
  }
  const name = def.join('_');
  return { name, keyPath: def };
}
