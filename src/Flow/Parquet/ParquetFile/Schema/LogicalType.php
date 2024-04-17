<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema\LogicalType\{Decimal, Time, Timestamp};
use Flow\Parquet\Thrift\TimeUnit;

final class LogicalType
{
    /**
     * @readonly
     * @var string
     */
    private $name;
    /**
     * @readonly
     * @var \Flow\Parquet\ParquetFile\Schema\LogicalType\Timestamp|null
     */
    private $timestamp;
    /**
     * @readonly
     * @var \Flow\Parquet\ParquetFile\Schema\LogicalType\Time|null
     */
    private $time;
    /**
     * @readonly
     * @var \Flow\Parquet\ParquetFile\Schema\LogicalType\Decimal|null
     */
    private $decimal;
    public const BSON = 'BSON';

    public const DATE = 'DATE';

    public const DECIMAL = 'DECIMAL';

    public const ENUM = 'ENUM';

    public const INTEGER = 'INTEGER';

    public const JSON = 'JSON';

    public const LIST = 'LIST';

    public const MAP = 'MAP';

    public const STRING = 'STRING';

    public const TIME = 'TIME';

    public const TIMESTAMP = 'TIMESTAMP';

    public const UNKNOWN = 'UNKNOWN';

    public const UUID = 'UUID';

    public function __construct(string $name, ?Timestamp $timestamp = null, ?Time $time = null, ?Decimal $decimal = null)
    {
        $this->name = $name;
        $this->timestamp = $timestamp;
        $this->time = $time;
        $this->decimal = $decimal;
    }

    public static function bson() : self
    {
        return new self(self::BSON);
    }

    public static function date() : self
    {
        return new self(self::DATE);
    }

    public static function decimal(int $scale, int $precision) : self
    {
        return new self(self::DECIMAL, null, null, new Decimal($scale, $precision));
    }

    public static function enum() : self
    {
        return new self(self::ENUM);
    }

    /**
     * @psalm-suppress RedundantConditionGivenDocblockType
     */
    public static function fromThrift(\Flow\Parquet\Thrift\LogicalType $logicalType) : self
    {
        $name = null;

        if ($logicalType->STRING !== null) {
            $name = self::STRING;
        }

        if ($logicalType->MAP !== null) {
            $name = self::MAP;
        }

        if ($logicalType->LIST !== null) {
            $name = self::LIST;
        }

        if ($logicalType->ENUM !== null) {
            $name = self::ENUM;
        }

        if ($logicalType->DECIMAL !== null) {
            $name = self::DECIMAL;
        }

        if ($logicalType->DATE !== null) {
            $name = self::DATE;
        }

        if ($logicalType->TIME !== null) {
            $name = self::TIME;
        }

        if ($logicalType->TIMESTAMP !== null) {
            $name = self::TIMESTAMP;
        }

        if ($logicalType->INTEGER !== null) {
            $name = self::INTEGER;
        }

        if ($logicalType->UNKNOWN !== null) {
            $name = self::UNKNOWN;
        }

        if ($logicalType->JSON !== null) {
            $name = self::JSON;
        }

        if ($logicalType->BSON !== null) {
            $name = self::BSON;
        }

        if ($logicalType->UUID !== null) {
            $name = self::UUID;
        }

        if (null === $name) {
            throw new InvalidArgumentException('Unknown logical type');
        }

        return new self(
            $name,
            $logicalType->TIMESTAMP !== null ? Timestamp::fromThrift($logicalType->TIMESTAMP) : null,
            $logicalType->TIME !== null ? Time::fromThrift($logicalType->TIME) : null,
            $logicalType->DECIMAL !== null ? Decimal::fromThrift($logicalType->DECIMAL) : null
        );
    }

    public static function integer() : self
    {
        return new self(self::INTEGER);
    }

    public static function json() : self
    {
        return new self(self::JSON);
    }

    public static function list() : self
    {
        return new self(self::LIST);
    }

    public static function map() : self
    {
        return new self(self::MAP);
    }

    public static function string() : self
    {
        return new self(self::STRING);
    }

    public static function time() : self
    {
        return new self(self::TIME, null, new Time(false, false, true, false));
    }

    public static function timestamp() : self
    {
        return new self(self::TIMESTAMP, new Timestamp(false, false, true, false));
    }

    public static function unknown() : self
    {
        return new self(self::UNKNOWN);
    }

    public static function uuid() : self
    {
        return new self(self::UUID);
    }

    public function decimalData() : ?Decimal
    {
        return $this->decimal;
    }

    public function is(string $logicalType) : bool
    {
        return $this->name() === $logicalType;
    }

    public function name() : string
    {
        return $this->name;
    }

    public function timeData() : ?Time
    {
        return $this->time;
    }

    public function timestampData() : ?Timestamp
    {
        return $this->timestamp;
    }

    public function toThrift() : \Flow\Parquet\Thrift\LogicalType
    {
        return new \Flow\Parquet\Thrift\LogicalType([
            self::BSON => $this->is(self::BSON) ? new \Flow\Parquet\Thrift\BsonType() : null,
            self::DATE => $this->is(self::DATE) ? new \Flow\Parquet\Thrift\DateType() : null,
            self::DECIMAL => $this->is(self::DECIMAL) ? new \Flow\Parquet\Thrift\DecimalType([
                'scale' => ($nullsafeVariable1 = $this->decimalData()) ? $nullsafeVariable1->scale() : null,
                'precision' => ($nullsafeVariable2 = $this->decimalData()) ? $nullsafeVariable2->precision() : null,
            ]) : null,
            self::ENUM => $this->is(self::ENUM) ? new \Flow\Parquet\Thrift\EnumType() : null,
            self::INTEGER => $this->is(self::INTEGER) ? new \Flow\Parquet\Thrift\IntType() : null,
            self::JSON => $this->is(self::JSON) ? new \Flow\Parquet\Thrift\JsonType() : null,
            self::LIST => $this->is(self::LIST) ? new \Flow\Parquet\Thrift\ListType() : null,
            self::MAP => $this->is(self::MAP) ? new \Flow\Parquet\Thrift\MapType() : null,
            self::STRING => $this->is(self::STRING) ? new \Flow\Parquet\Thrift\StringType() : null,
            self::TIME => $this->is(self::TIME) ? new \Flow\Parquet\Thrift\TimeType([
                'isAdjustedToUTC' => ($nullsafeVariable3 = $this->timeData()) ? $nullsafeVariable3->isAdjustedToUTC() : null,
                'unit' => new TimeUnit([
                    'MILLIS' => (($nullsafeVariable4 = $this->timeData()) ? $nullsafeVariable4->millis() : null) ? new \Flow\Parquet\Thrift\MilliSeconds() : null,
                    'MICROS' => (($nullsafeVariable5 = $this->timeData()) ? $nullsafeVariable5->micros() : null) ? new \Flow\Parquet\Thrift\MicroSeconds() : null,
                    'NANOS' => (($nullsafeVariable6 = $this->timeData()) ? $nullsafeVariable6->nanos() : null) ? new \Flow\Parquet\Thrift\NanoSeconds() : null,
                ]),
            ]) : null,
            self::TIMESTAMP => $this->is(self::TIMESTAMP) ? new \Flow\Parquet\Thrift\TimestampType([
                'isAdjustedToUTC' => ($nullsafeVariable7 = $this->timestampData()) ? $nullsafeVariable7->isAdjustedToUTC() : null,
                'unit' => new TimeUnit([
                    'MILLIS' => (($nullsafeVariable8 = $this->timestampData()) ? $nullsafeVariable8->millis() : null) ? new \Flow\Parquet\Thrift\MilliSeconds() : null,
                    'MICROS' => (($nullsafeVariable9 = $this->timestampData()) ? $nullsafeVariable9->micros() : null) ? new \Flow\Parquet\Thrift\MicroSeconds() : null,
                    'NANOS' => (($nullsafeVariable10 = $this->timestampData()) ? $nullsafeVariable10->nanos() : null) ? new \Flow\Parquet\Thrift\NanoSeconds() : null,
                ]),
            ]) : null,
            self::UNKNOWN => $this->is(self::UNKNOWN) ? new \Flow\Parquet\Thrift\NullType() : null,
            self::UUID => $this->is(self::UUID) ? new \Flow\Parquet\Thrift\UUIDType() : null,
        ]);
    }
}
