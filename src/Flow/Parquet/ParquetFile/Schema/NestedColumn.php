<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Thrift\SchemaElement;

final class NestedColumn implements Column
{
    /**
     * @readonly
     * @var string
     */
    private $name;
    /**
     * @var \Flow\Parquet\ParquetFile\Schema\Repetition|null
     */
    private $repetition;
    /**
     * @var array<Column>
     * @readonly
     */
    private $children;
    /**
     * @readonly
     * @var \Flow\Parquet\ParquetFile\Schema\ConvertedType|null
     */
    private $convertedType;
    /**
     * @readonly
     * @var \Flow\Parquet\ParquetFile\Schema\LogicalType|null
     */
    private $logicalType;
    /**
     * @readonly
     * @var bool
     */
    public $schemaRoot = false;
    /**
     * @var string|null
     */
    private $flatPath;

    /**
     * @var $this|null
     */
    private $parent;

    /**
     * @param array<Column> $children
     * @param ?\Flow\Parquet\ParquetFile\Schema\ConvertedType::* $convertedType
     */
    public function __construct(
        string $name,
        ?Repetition $repetition,
        array $children,
        $convertedType = null,
        ?LogicalType $logicalType = null,
        bool $schemaRoot = false
    ) {
        $this->name = $name;
        $this->repetition = $repetition;
        $this->children = $children;
        $this->convertedType = $convertedType;
        $this->logicalType = $logicalType;
        $this->schemaRoot = $schemaRoot;
        foreach ($children as $child) {
            $child->setParent($this);
        }
    }

    /**
     * @param array<Column> $columns
     */
    public static function create(string $name, array $columns) : self
    {
        return new self($name, Repetition::OPTIONAL, $columns);
    }

    /**
     * @psalm-suppress RedundantConditionGivenDocblockType
     *
     * @param array<Column> $children
     */
    public static function fromThrift(SchemaElement $schemaElement, array $children) : self
    {
        return new self(
            $schemaElement->name,
            $schemaElement->repetition_type ? Repetition::from($schemaElement->repetition_type) : null,
            $children,
            $schemaElement->converted_type ? ConvertedType::from($schemaElement->converted_type) : null,
            $schemaElement->logicalType ? LogicalType::fromThrift($schemaElement->logicalType) : null
        );
    }

    public static function list(string $name, ListElement $element) : self
    {
        return new self(
            $name,
            Repetition::OPTIONAL,
            [
                new self(
                    'list',
                    Repetition::REPEATED,
                    [$element->element]
                ),
            ],
            ConvertedType::LIST,
            new LogicalType(LogicalType::LIST)
        );
    }

    public static function map(string $name, MapKey $key, MapValue $value) : self
    {
        return new self(
            $name,
            Repetition::OPTIONAL,
            [
                new self('key_value', Repetition::REPEATED, [
                    $key->key,
                    $value->value,
                ]),
            ],
            ConvertedType::MAP,
            new LogicalType(LogicalType::MAP)
        );
    }

    /**
     * @param array<Column> $children
     */
    public static function schemaRoot(string $name, array $children) : self
    {
        return new self($name, Repetition::REQUIRED, $children, null, null, true);
    }

    /**
     * @param array<Column> $children
     */
    public static function struct(string $name, array $children) : self
    {
        return new self($name, Repetition::OPTIONAL, $children);
    }

    /**
     * @return array<Column>
     */
    public function children() : array
    {
        return $this->children;
    }

    /**
     * @return array<string, FlatColumn>
     */
    public function childrenFlat() : array
    {
        $flat = [];

        foreach ($this->children as $child) {
            if ($child instanceof self) {
                $flat = \array_merge($flat, $child->childrenFlat());
            } else {
                /** @var FlatColumn $child */
                $flat[$child->flatPath()] = $child;
            }
        }

        return $flat;
    }

    public function ddl() : array
    {
        $ddlArray = [
            'type' => 'group',
            'optional' => (($nullsafeVariable1 = $this->repetition()) ? $nullsafeVariable1->value : null) === Repetition::OPTIONAL->value,
            'children' => [],
        ];

        foreach ($this->children as $column) {
            $ddlArray['children'][$column->name()] = $column->ddl();
        }

        return $ddlArray;
    }

    public function flatPath() : string
    {
        if ($this->flatPath !== null) {
            return $this->flatPath;
        }

        $parent = $this->parent();

        if (($nullsafeVariable2 = $parent) ? $nullsafeVariable2->schemaRoot : null) {
            $this->flatPath = $this->name;

            return $this->flatPath;
        }

        $path = [$this->name];

        while ($parent) {
            $path[] = $parent->name();
            $parent = $parent->parent();

            if ($parent && $parent->schemaRoot) {
                break;
            }
        }

        $path = \array_reverse($path);
        $this->flatPath = \implode('.', $path);

        return $this->flatPath;
    }

    /**
     * @psalm-suppress UndefinedInterfaceMethod
     */
    public function getListElement() : Column
    {
        if ($this->isList()) {
            /** @phpstan-ignore-next-line */
            return $this->children()[0]->children()[0];
        }

        throw new InvalidArgumentException('Column ' . $this->flatPath() . ' is not a list');
    }

    /**
     * @psalm-suppress UndefinedInterfaceMethod
     */
    public function getMapKeyColumn() : FlatColumn
    {
        if ($this->isMap()) {
            /** @phpstan-ignore-next-line */
            return $this->children()[0]->children()[0];
        }

        throw new InvalidArgumentException('Column ' . $this->flatPath() . ' is not a map');
    }

    /**
     * @psalm-suppress UndefinedInterfaceMethod
     */
    public function getMapValueColumn() : Column
    {
        if ($this->isMap()) {
            /** @phpstan-ignore-next-line */
            return $this->children()[0]->children()[1];
        }

        throw new InvalidArgumentException('Column ' . $this->flatPath() . ' is not a map');
    }

    public function isList() : bool
    {
        return (($nullsafeVariable3 = $this->logicalType()) ? $nullsafeVariable3->name() : null) === 'LIST';
    }

    public function isListElement() : bool
    {
        if ($this->parent !== null) {
            // element
            if ((($nullsafeVariable4 = $this->parent->logicalType()) ? $nullsafeVariable4->name() : null) === 'LIST') {
                return true;
            }

            // list.element
            if ((($nullsafeVariable5 = ($nullsafeVariable6 = $this->parent->parent()) ? $nullsafeVariable6->logicalType() : null) ? $nullsafeVariable5->name() : null) === 'LIST') {
                return true;
            }
        }

        return false;
    }

    public function isMap() : bool
    {
        return (($nullsafeVariable7 = $this->logicalType()) ? $nullsafeVariable7->name() : null) === 'MAP';
    }

    public function isMapElement() : bool
    {
        if ($this->parent === null) {
            return false;
        }

        if ((($nullsafeVariable8 = ($nullsafeVariable9 = $this->parent()) ? $nullsafeVariable9->logicalType() : null) ? $nullsafeVariable8->name() : null) === 'MAP') {
            return true;
        }

        if ((($nullsafeVariable10 = ($nullsafeVariable11 = ($nullsafeVariable12 = $this->parent()) ? $nullsafeVariable12->parent() : null) ? $nullsafeVariable11->logicalType() : null) ? $nullsafeVariable10->name() : null) === 'MAP') {
            return true;
        }

        return false;
    }

    public function isRequired() : bool
    {
        return $this->repetition !== Repetition::OPTIONAL;
    }

    public function isStruct() : bool
    {
        if ($this->isMap()) {
            return false;
        }

        if ($this->isList()) {
            return false;
        }

        return true;
    }

    public function isStructElement() : bool
    {
        if ($this->isMapElement()) {
            return false;
        }

        if ($this->isListElement()) {
            return false;
        }

        return true;
    }

    public function logicalType() : ?LogicalType
    {
        return $this->logicalType;
    }

    public function makeRequired() : self
    {
        $this->repetition = Repetition::REQUIRED;

        return $this;
    }

    public function maxDefinitionsLevel() : int
    {
        if ($this->repetition === null) {
            $level = 0;
        } else {
            $level = $this->repetition() === Repetition::REQUIRED ? 0 : 1;
        }

        return $this->parent ? $level + $this->parent->maxDefinitionsLevel() : $level;
    }

    public function maxRepetitionsLevel() : int
    {
        if ($this->repetition === null) {
            $level = 0;
        } else {
            $level = $this->repetition() === Repetition::REPEATED ? 1 : 0;
        }

        return $this->parent ? $level + $this->parent->maxRepetitionsLevel() : $level;
    }

    public function name() : string
    {
        return $this->name;
    }

    public function parent() : ?\Flow\Parquet\ParquetFile\Schema\NestedColumn
    {
        return $this->parent;
    }

    public function path() : array
    {
        return \explode('.', $this->flatPath());
    }

    public function repetition() : ?Repetition
    {
        return $this->repetition;
    }

    /**
     * @param $this $parent
     */
    public function setParent($parent) : void
    {
        $this->flatPath = null;
        $this->parent = $parent;

        foreach ($this->children as $child) {
            $child->setParent($this);
        }
    }

    /**
     * @return array<SchemaElement>
     */
    public function toThrift() : array
    {
        $elements = [
            new SchemaElement([
                'name' => $this->name(),
                'num_children' => \count($this->children),
                'converted_type' => ($nullsafeVariable13 = $this->convertedType) ? $nullsafeVariable13->value : null,
                'repetition_type' => ($nullsafeVariable14 = $this->repetition()) ? $nullsafeVariable14->value : null,
                'logicalType' => ($nullsafeVariable15 = $this->logicalType()) ? $nullsafeVariable15->toThrift() : null,
            ]),
        ];

        foreach ($this->children as $child) {
            if ($child instanceof FlatColumn) {
                $elements[] = $child->toThrift();
            }

            if ($child instanceof self) {
                $elements = \array_merge($elements, $child->toThrift());
            }
        }

        return $elements;
    }

    public function type() : ?PhysicalType
    {
        return null;
    }

    public function typeLength() : ?int
    {
        return null;
    }
}
