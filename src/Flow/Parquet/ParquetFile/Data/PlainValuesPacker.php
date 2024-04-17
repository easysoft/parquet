<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data;

use Flow\Parquet\BinaryWriter;
use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, LogicalType, PhysicalType};

final class PlainValuesPacker
{
    /**
     * @readonly
     * @var \Flow\Parquet\BinaryWriter
     */
    private $writer;
    /**
     * @readonly
     * @var \Flow\Parquet\Data\DataConverter
     */
    private $dataConverter;
    public function __construct(BinaryWriter $writer, DataConverter $dataConverter)
    {
        $this->writer = $writer;
        $this->dataConverter = $dataConverter;
    }
    public function packValues(FlatColumn $column, array $values) : void
    {
        $parquetValues = [];

        foreach ($values as $value) {
            if ($value === null) {
                continue;
            }

            $parquetValues[] = $this->dataConverter->toParquetType($column, $value);
        }

        switch ($column->type()) {
            case PhysicalType::BOOLEAN:
                $this->writer->writeBooleans($parquetValues);

                break;
            case PhysicalType::INT32:
                switch (($nullsafeVariable1 = $column->logicalType()) ? $nullsafeVariable1->name() : null) {
                    case LogicalType::DATE:
                        $this->writer->writeInts32($parquetValues);

                        break;
                    case null:
                        $this->writer->writeInts32($parquetValues);

                        break;
                }

                break;
            case PhysicalType::INT64:
                switch (($nullsafeVariable2 = $column->logicalType()) ? $nullsafeVariable2->name() : null) {
                    case LogicalType::TIME:
                    case LogicalType::TIMESTAMP:
                        $this->writer->writeInts64($parquetValues);

                        break;
                    case null:
                        $this->writer->writeInts64($parquetValues);

                        break;
                }

                break;
            case PhysicalType::FLOAT:
                $this->writer->writeFloats($parquetValues);

                break;
            case PhysicalType::DOUBLE:
                $this->writer->writeDoubles($parquetValues);

                break;
            case PhysicalType::FIXED_LEN_BYTE_ARRAY:
                switch (($nullsafeVariable3 = $column->logicalType()) ? $nullsafeVariable3->name() : null) {
                    case LogicalType::DECIMAL:
                        /**
                         * @phpstan-ignore-next-line
                         *
                         * @psalm-suppress PossiblyNullArgument
                         */
                        $this->writer->writeDecimals($parquetValues, $column->typeLength(), $column->precision(), $column->scale());

                        break;

                    default:
                        throw new \RuntimeException('Writing logical type "' . ((($nullsafeVariable4 = $column->logicalType()) ? $nullsafeVariable4->name() : null) ?: 'UNKNOWN') . '" is not implemented yet');
                }

                break;
            case PhysicalType::BYTE_ARRAY:
                switch (($nullsafeVariable5 = $column->logicalType()) ? $nullsafeVariable5->name() : null) {
                    case LogicalType::UUID:
                    case LogicalType::JSON:
                    case LogicalType::STRING:
                        $this->writer->writeStrings($parquetValues);

                        break;

                    default:
                        throw new \RuntimeException('Writing logical type "' . ((($nullsafeVariable6 = $column->logicalType()) ? $nullsafeVariable6->name() : null) ?: 'UNKNOWN') . '" is not implemented yet');
                }

                break;

            default:
                throw new \RuntimeException('Writing physical type "' . $column->type()->name . '" is not implemented yet');
        }
    }
}
