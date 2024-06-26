<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\Validator;

use Flow\Parquet\Exception\ValidationException;
use Flow\Parquet\ParquetFile\RowGroupBuilder\Validator;
use Flow\Parquet\ParquetFile\Schema\{Column, FlatColumn, LogicalType, PhysicalType, Repetition};

final class ColumnDataValidator implements Validator
{
    /**
     * @param mixed $data
     */
    public function validate(Column $column, $data) : void
    {
        if ($column->repetition() === Repetition::REQUIRED) {
            if ($data === null) {
                throw new ValidationException(\sprintf('Column "%s" is required', $column->name()));
            }
        }

        if ($column instanceof FlatColumn) {
            $this->validateData($column, $data);
        }
    }

    /**
     * @param mixed $data
     */
    private function validateData(FlatColumn $column, $data) : void
    {
        if (\is_array($data)) {
            foreach ($data as $value) {
                $this->validateData($column, $value);
            }

            return;
        }

        if ($column->repetition() !== Repetition::REQUIRED) {
            if ($data === null) {
                return;
            }
        }

        switch ($column->type()) {
            case PhysicalType::BOOLEAN:
                if (!\is_bool($data)) {
                    throw new ValidationException(\sprintf('Column "%s" is not boolean', $column->flatPath()));
                }

                break;
            case PhysicalType::INT64:
            case PhysicalType::INT32:
                switch (($nullsafeVariable1 = $column->logicalType()) ? $nullsafeVariable1->name() : null) {
                    case LogicalType::DATE:
                    case LogicalType::TIMESTAMP:
                        if (!$data instanceof \DateTimeInterface) {
                            throw new ValidationException(\sprintf('Column "%s" require \DateTimeInterface as value', $column->flatPath()));
                        }

                        break;
                    case LogicalType::TIME:
                        if (!$data instanceof \DateInterval) {
                            throw new ValidationException(\sprintf('Column "%s" require \DateInterval as value', $column->flatPath()));
                        }

                        break;
                    case null:
                        if (!\is_int($data)) {
                            throw new ValidationException(\sprintf('Column "%s" require integer as value, got: %s instead', $column->flatPath(), \gettype($data)));
                        }

                        break;
                }

                break;
            case PhysicalType::FLOAT:
            case PhysicalType::DOUBLE:
                if (!\is_float($data)) {
                    throw new ValidationException(\sprintf('Column "%s" is not float', $column->flatPath()));
                }

                break;
            case PhysicalType::BYTE_ARRAY:
                switch (($nullsafeVariable2 = $column->logicalType()) ? $nullsafeVariable2->name() : null) {
                    case LogicalType::STRING:
                    case LogicalType::JSON:
                    case LogicalType::UUID:
                        if (!\is_string($data)) {
                            throw new ValidationException(\sprintf('Column "%s" is not string, got "%s" instead', $column->flatPath(), \gettype($data)));
                        }

                        break;
                }

                break;
            case PhysicalType::FIXED_LEN_BYTE_ARRAY:
                break;

            default:
                throw new ValidationException(\sprintf('Unknown column type "%s"', $column->type()->name));
        }
    }
}
