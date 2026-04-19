# Руководство дата-инженера: eMondrian Кондитерка

**Версия**: spike/jdk25 @ 3566303de  
**Дата**: 2026-04-19  
**Docker image**: `cr.yandex/crp4jptegc7vdt2icfag/emondrian-clickhouse:flat-hier-bcf99ced5`

---

## 1. Обязательные настройки

В `setenv.sh` (или `CATALINA_OPTS`):

```bash
CATALINA_OPTS="$CATALINA_OPTS -Dmondrian.native.queryEngine.enable=true"
CATALINA_OPTS="$CATALINA_OPTS -Dmondrian.native.nonEmptyFilter.enable=true"
```

| Свойство | Default | Рекомендация | Эффект |
|----------|---------|-------------|--------|
| `mondrian.native.queryEngine.enable` | `false` | **`true`** | NQE: query-wide SQL pushdown + agg-table routing + prefetch coexistence |
| `mondrian.native.nonEmptyFilter.enable` | `false` | **`true`** | NNEF: SQL pre-filter для NON EMPTY crossjoin (q46: −87%) |

Остальные `mondrian.native.*` свойства оставить по умолчанию.

---

## 2. Новые возможности схемы

### 2.1. `flatName` на Level

Публикует уровень иерархии как отдельное плоское поле для Excel:

```xml
<Hierarchy name="Товар" showHierarchy="false" hasAll="true" primaryKey="sku_key">
  <Table name="dim_konfet_product"/>
  <Level name="Категория" column="category" flatName="Категория"/>
  <Level name="Подкатегория" column="subcategory" flatName="Подкатегория"/>
  <Level name="СКЮ" column="sku_key" flatName="СКЮ"/>
</Hierarchy>

<Hierarchy name="Марка" showHierarchy="false" hasAll="true" primaryKey="sku_key">
  <Table name="dim_konfet_product"/>
  <Level name="Производитель" column="manufacturer_group" flatName="Производитель"/>
  <Level name="Бренд" column="brand" flatName="Бренд"/>
  <Level name="СКЮ" column="sku_key" flatName="СКЮ"/>
</Hierarchy>
```

**Что происходит**:
- Каждый `flatName` создаёт synthetic single-level hierarchy, доступную в Excel и MDX
- СКЮ автоматически дедуплицируется — одно поле вместо двух
- Drill-иерархии (Товар, Марка) скрыты из field list (`showHierarchy="false"`), но работают в MDX
- Crossjoin pruning работает автоматически через source links (без `drilldown.dependsOn` аннотаций)

**Дедупликация**: по canonical identity (`table + column` внутри dimension). Если СКЮ есть и в Товар и в Марка — создаётся один flat field, оба источника регистрируются для pruning.

### 2.2. `showHierarchy` на Hierarchy

```xml
<Hierarchy name="Товар" showHierarchy="false">
```

- `true` (default) — иерархия видна в Excel field list
- `false` — скрыта из discovery, но MDX и внутренняя логика работают

### 2.3. Миграция с дублирующих flat-иерархий

**Было** (дублирование):
```xml
<!-- Drill hierarchy -->
<Hierarchy name="Товар"><Level name="Категория"/><Level name="Подкатегория"/><Level name="СКЮ"/></Hierarchy>
<!-- Duplicate flat hierarchies -->
<Hierarchy name="Категория"><Level name="Категория" column="category"/></Hierarchy>
<Hierarchy name="Подкатегория"><Level name="Подкатегория" column="subcategory"/></Hierarchy>
<Hierarchy name="СКЮ"><Level name="СКЮ" column="sku_key"/></Hierarchy>
```

**Стало** (единый источник):
```xml
<Hierarchy name="Товар" showHierarchy="false">
  <Level name="Категория" column="category" flatName="Категория"/>
  <Level name="Подкатегория" column="subcategory" flatName="Подкатегория"/>
  <Level name="СКЮ" column="sku_key" flatName="СКЮ"/>
</Hierarchy>
<!-- Дублирующие flat-иерархии УДАЛЕНЫ -->
```

**Преимущества**:
- Один `AggLevel` mapping вместо двух
- Нет NNEF `UNRESOLVABLE_HIERARCHY`
- Нет дублирования member identity
- Crossjoin pruning автоматический

### 2.4. Auto-pruning вместо DrillDep аннотаций

**Было**:
```xml
<Level name="Бренд" column="brand">
  <Annotations>
    <Annotation name="drilldown.dependsOn">[Производитель]</Annotation>
  </Annotations>
</Level>
```

**Стало**: не нужно. Pruning выводится автоматически из source links:
- `[Бренд(flat)]` → source = Марка, depth=1
- `[Производитель(flat)]` → source = Марка, depth=0
- Общая иерархия Марка, depth 1 > 0 → ancestor dependency → prune

**Ограничение**: property для ancestor key должен существовать на уровне member. Для СКЮ уровня добавьте properties:

```xml
<Level name="СКЮ" column="sku_key" flatName="СКЮ">
  <Property name="Категория" column="category" dependsOnLevelValue="true"/>
  <Property name="Подкатегория" column="subcategory" dependsOnLevelValue="true"/>
  <Property name="Производитель" column="manufacturer_group" dependsOnLevelValue="true"/>
  <Property name="Бренд" column="brand" dependsOnLevelValue="true"/>
</Level>
```

`drilldown.dependsOn` аннотации по-прежнему работают как fallback для нестандартных зависимостей.

---

## 3. Производительность

Результаты на Кондитерка regression pack (49 запросов, warm):

| Метрика | Legacy | NQE+NNEF | Улучшение |
|---------|--------|----------|-----------|
| **Общее время** | 20.4s | 15.4s | **−24%** |
| **q46** (DrilldownMember WD) | 3.52s | 0.47s | **−87%** |
| **q12** (address × category) | 5.68s | 3.97s | **−30%** |
| **Длинные (>0.3s)** | 14.2s | 9.0s | **−37%** |

### Что делает NQE (NativeQueryEngine)

- Для stored-only запросов: полный SQL pushdown через agg tables
- Для mixed запросов (stored + WD%): prefetch stored measures, legacy обрабатывает WD%
- Автоматический выбор agg table через `NqeTableStrategy` (аналог `findAgg`)

### Что делает NNEF (NativeNonEmptyFilter)

- SQL pre-filter для NON EMPTY crossjoin: `SELECT DISTINCT dims FROM fact GROUP BY dims HAVING ...`
- Порог: минимум 100 кандидатов (не тратит SQL на мелкие crossjoin)
- Результат фильтрации в HashSet → O(1) lookup на tuple

---

## 4. Agg Tables

### Рекомендации для agg tables с `flatName`

С `flatName` достаточно **одного** `AggLevel` mapping через реальную иерархию:

```xml
<AggLevel name="[Продукт.Товар].[Категория]" column="category"/>
```

Старый дублирующий маппинг для flat-иерархии **не нужен**:
```xml
<!-- НЕ НУЖЕН при использовании flatName -->
<AggLevel name="[Продукт.Категория].[Категория]" column="category"/>
```

Однако если он есть — продолжает работать (backward compat).

### HLL merge measures

Agg tables с HLL state columns (`akb_state`, `sku_count_state`) правильно обрабатываются:
- NQE: `uniqCombinedMerge(agg.akb_state)` через `AggResolvedTable`
- Legacy: `AggStar.FactTable.Measure.generateRollupString()`

---

## 5. NativeSqlCalc (WD%, template measures)

### Кириллические колонки (#53)

Исправлено: `Dialect.quoteIdentifier()` применяется ко всем column/table identifiers в template variable substitution. Колонки вида `goods.Поставщик` корректно квотируются как `` goods.`Поставщик` ``.

### Template annotations

Не изменились. `nativeSql.template` работает как прежде:

```xml
<Annotation name="nativeSql.enabled">true</Annotation>
<Annotation name="nativeSql.template"><![CDATA[
  SELECT ${axisResultSelectList}
    uniqCombinedMerge(store_state) AS val
  FROM ...
]]></Annotation>
```

---

## 6. Docker images

| Tag | Содержимое |
|-----|-----------|
| `nqe-nnef-b02288510` | NQE Phase 2A+2B + NNEF + Issue #53 fix |
| `flat-hier-bcf99ced5` | + flatName/showHierarchy + auto-pruning |

Registry: `cr.yandex/crp4jptegc7vdt2icfag/emondrian-clickhouse`

---

## 7. Чеклист для нового куба

1. ✅ Определить drill-иерархии с `showHierarchy="false"` и `flatName` на уровнях
2. ✅ Добавить properties на leaf-level для auto-pruning (parent level columns)
3. ✅ Удалить дублирующие single-level flat-иерархии
4. ✅ Удалить `drilldown.dependsOn` аннотации (если покрыты auto-pruning)
5. ✅ Один `AggLevel` per реальный level (не дублировать для flat)
6. ✅ Включить `queryEngine.enable=true` + `nonEmptyFilter.enable=true`
7. ✅ Прогнать regression pack, проверить XMLA discovery
