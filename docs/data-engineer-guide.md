# Руководство дата-инженера: eMondrian Кондитерка

**Версия**: NQE + NNEF + SchemaEditor Vue 3 migration
**Дата**: 2026-05-11
**Docker image**: `cr.yandex/crp4jptegc7vdt2icfag/emondrian-clickhouse:nqe-vue3-316e404`

---

## 1. Обязательные настройки

В `config/mondrian.properties`:

```properties
mondrian.native.queryEngine.enable=true
mondrian.native.nonEmptyFilter.enable=true
mondrian.native.sql.enable=true
mondrian.rolap.queryTimeout=300
```

| Свойство | Default | Рекомендация | Эффект |
|----------|---------|-------------|--------|
| `mondrian.native.queryEngine.enable` | `true` | **явно `true`** | NQE: query-wide SQL pushdown + agg-table routing + prefetch coexistence |
| `mondrian.native.nonEmptyFilter.enable` | `false` | **`true`** | NNEF: SQL pre-filter для NON EMPTY crossjoin (q46: −87%) |
| `mondrian.native.sql.enable` | `false` | **`true`, если schema использует `nativeSql.*`** | SQL-шаблоны для calculated measures |
| `mondrian.rolap.queryTimeout` | `0` | **`300` как стартовый профиль** | timeout теперь применяется и к native SQL path |

`native.queryEngine.enable` сейчас включён по умолчанию в коде, но для продакшена
его нужно задавать явно: это делает rollout/rollback видимым в diff конфигурации.

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

### 2.4. Dependency pruning и validator

Текущий production-standard: schema validator должен быть чистым. Для плоских
иерархий предпочтительны явные `drilldown.dependsOn` или
`drilldown.dependsOnChain` с property mapping.

Если у `Property` стоит `dependsOnLevelValue="true"`, но нет соответствующей
dependency-аннотации, validator выдаёт `PROPERTY_FLAG_WITHOUT_DEPENDS_ON`.
Такой warning нужно либо исправить аннотацией, либо убрать флаг с property.

#### Auto-pruning вместо DrillDep аннотаций

**Было**:
```xml
<Level name="Бренд" column="brand">
  <Annotations>
    <Annotation name="drilldown.dependsOn">[Производитель]</Annotation>
  </Annotations>
</Level>
```

**Для source-link flatName моделей** pruning может выводиться автоматически из
source links:
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

`drilldown.dependsOn` аннотации по-прежнему нужны для нестандартных
зависимостей и для случаев, где validator не может однозначно вывести property.

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

Конфигурация:

```properties
mondrian.rolap.aggregates.DistinctCountMergeFunction=uniqCombinedMerge
mondrian.rolap.aggregates.DistinctCountMergeMode=auto
mondrian.rolap.aggregates.DistinctCountMergeAllowConstrainedRollup=true
# Optional allow-list: keys are measure names, not physical state-column names.
#mondrian.rolap.aggregates.DistinctCountMergeFunctionMap=AKB=uniqCombinedMerge,SKU=uniqCombinedMerge
```

`DistinctCountMergeColumns` не используется текущим engine. Если задан
`DistinctCountMergeFunctionMap`, только перечисленные меры используют
merge-state routing; остальные distinct меры идут обычным путём.

---

## 5. NativeSqlCalc (WD%, template measures)

### Кириллические колонки (#53)

Исправлено: `Dialect.quoteIdentifier()` применяется ко всем column/table identifiers в template variable substitution. Колонки вида `goods.Поставщик` корректно квотируются как `` goods.`Поставщик` ``.

### Template annotations

`nativeSql.template` работает как прежде, но текущий контракт включает
fallback templates, `relationAlias`, `scalar` и `rollupAxes`:

```xml
<Annotation name="nativeSql.enabled">true</Annotation>
<Annotation name="nativeSql.maxAxes">8</Annotation>
<Annotation name="nativeSql.template"><![CDATA[
  SELECT ${axisResultSelectList}
    uniqCombinedMerge(store_state) AS val
  FROM ...
]]></Annotation>
```

Если `nativeSql.rollupAxes=true`, каждый template обязан содержать оба macro:
`${axisGroupByListCube}` и `${axisCubeSelectFlags}`.

---

## 6. Docker images

| Tag | Содержимое |
|-----|-----------|
| `nqe-vue3-316e404` | Current: NQE/NNEF fixes + SchemaEditor Vue 3/Vite build included |
| `nqe-nnef-b02288510` | NQE Phase 2A+2B + NNEF + Issue #53 fix |
| `flat-hier-bcf99ced5` | + flatName/showHierarchy + auto-pruning |

Registry: `cr.yandex/crp4jptegc7vdt2icfag/emondrian-clickhouse`

---

## 7. Чеклист для нового куба

1. ✅ Определить drill-иерархии с `showHierarchy="false"` и `flatName` на уровнях
2. ✅ Добавить properties на leaf-level для auto-pruning (parent level columns)
3. ✅ Удалить дублирующие single-level flat-иерархии
4. ✅ Добавить явные `drilldown.dependsOn` / `dependsOnChain` там, где validator не выводит связь безопасно
5. ✅ Один `AggLevel` per реальный level (не дублировать для flat)
6. ✅ Включить явно `queryEngine.enable=true`, `nonEmptyFilter.enable=true`, `queryTimeout`
7. ✅ Прогнать schema validator: `fatal=0`, warnings осознанно исправлены или приняты
8. ✅ Прогнать regression pack, проверить XMLA discovery
