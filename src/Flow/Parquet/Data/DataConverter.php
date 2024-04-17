<?php

declare(strict_types=1);

namespace Flow\Parquet\Data;

use Flow\Parquet\Data\Converter\{BytesStringConverter, Int32DateConverter, Int32DateTimeConverter, Int64DateTimeConverter, Int96DateTimeConverter, JsonConverter, TimeConverter, UuidConverter};
use Flow\Parquet\Exception\DataConversionException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final class DataConverter
{
    /**
     * @var array<Converter>
     * @readonly
     */
    private $converters;
    /**
     * @readonly
     * @var \Flow\Parquet\Options
     */
    private $options;
    /**
     * @var array<string, null|Converter>
     */
    private $cache;

    /**
     * @param array<Converter> $converters
     */
    public function __construct(array $converters, Options $options)
    {
        $this->converters = $converters;
        $this->options = $options;
        $this->cache = [];
    }

    public static function initialize(Options $options) : self
    {
        return new self(
            [
                new TimeConverter(),
                new Int32DateConverter(),
                new Int32DateTimeConverter(),
                new Int64DateTimeConverter(),
                new Int96DateTimeConverter(),
                new BytesStringConverter(),
                new UuidConverter(),
                new JsonConverter(),
            ],
            $options
        );
    }

    /**
     * @param mixed $data
     * @return mixed
     */
    public function fromParquetType(FlatColumn $column, $data)
    {
        if ($data === null) {
            return null;
        }

        if (\array_key_exists($column->flatPath(), $this->cache)) {
            if ($this->cache[$column->flatPath()] === null) {
                return $data;
            }

            /** @psalm-suppress PossiblyNullReference */
            return $this->cache[$column->flatPath()]->fromParquetType($data);
        }

        foreach ($this->converters as $converter) {
            if ($converter->isFor($column, $this->options)) {
                $this->cache[$column->flatPath()] = $converter;

                try {
                    return $converter->fromParquetType($data);
                } catch (\Throwable $e) {
                    throw new DataConversionException(
                        "Failed to convert data from parquet type for column '{$column->flatPath()}'. {$e->getMessage()}",
                        0,
                        $e
                    );
                }
            }
        }

        $this->cache[$column->flatPath()] = null;

        return $data;
    }

    /**
     * @param mixed $data
     * @return mixed
     */
    public function toParquetType(FlatColumn $column, $data)
    {
        if (\array_key_exists($column->flatPath(), $this->cache)) {
            if ($this->cache[$column->flatPath()] === null) {
                return $data;
            }

            /** @psalm-suppress PossiblyNullReference */
            return $this->cache[$column->flatPath()]->toParquetType($data);
        }

        foreach ($this->converters as $converter) {
            if ($converter->isFor($column, $this->options)) {
                $this->cache[$column->flatPath()] = $converter;

                return $converter->toParquetType($data);
            }
        }

        $this->cache[$column->flatPath()] = null;

        return $data;
    }
}
