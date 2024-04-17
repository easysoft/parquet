<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\BinaryReader\BinaryBufferReader;
use Flow\Parquet\ByteOrder;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Data\{BitWidth, PlainValueUnpacker, RLEBitPackedHybrid};
use Flow\Parquet\ParquetFile\Page\Header\{DataPageHeader, DataPageHeaderV2, DictionaryPageHeader};
use Flow\Parquet\ParquetFile\Page\{ColumnData, Dictionary};
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final class DataCoder
{
    /**
     * @readonly
     * @var \Flow\Parquet\ByteOrder
     */
    private $byteOrder = ByteOrder::LITTLE_ENDIAN;
    /**
     * @param \Flow\Parquet\ByteOrder::* $byteOrder
     */
    public function __construct(string $byteOrder = ByteOrder::LITTLE_ENDIAN)
    {
        $this->byteOrder = $byteOrder;
    }

    public function decodeData(
        string $buffer,
        FlatColumn $column,
        DataPageHeader $pageHeader,
        ?Dictionary $dictionary = null
    ) : ColumnData {
        $reader = new BinaryBufferReader($buffer, $this->byteOrder);

        $RLEBitPackedHybrid = new RLEBitPackedHybrid();

        if ($column->maxRepetitionsLevel()) {
            $reader->readInts32(1); // read length of encoded data
            $repetitions = $this->readRLEBitPackedHybrid($reader, $RLEBitPackedHybrid, BitWidth::calculate($column->maxRepetitionsLevel()), $pageHeader->valuesCount());
        } else {
            $repetitions = [];
        }

        if ($column->maxDefinitionsLevel()) {
            $reader->readInts32(1); // read length of encoded data
            $definitions = $this->readRLEBitPackedHybrid($reader, $RLEBitPackedHybrid, BitWidth::calculate($column->maxDefinitionsLevel()), $pageHeader->valuesCount());
        } else {
            $definitions = [];
        }

        $nullsCount = \count($definitions) ? \count(\array_filter($definitions, static function ($definition) {
            return $definition === 0;
        })) : 0;

        if ($pageHeader->encoding() === Encodings::PLAIN) {
            return new ColumnData(
                $column->type(),
                $column->logicalType(),
                $repetitions,
                $definitions,
                (new PlainValueUnpacker($reader))->unpack($column, $pageHeader->valuesCount() - $nullsCount)
            );
        }

        if ($pageHeader->encoding() === Encodings::RLE_DICTIONARY || $pageHeader->encoding() === Encodings::PLAIN_DICTIONARY) {
            if (\count($definitions)) {
                // while reading indices, there is no length at the beginning since length is simply a remaining length of the buffer
                // however we need to know bitWidth which is the first value in the buffer after definitions
                $bitWidth = $reader->readBytes(1)->toInt();
                /** @var array<int> $indices */
                $indices = $this->readRLEBitPackedHybrid($reader, $RLEBitPackedHybrid, $bitWidth, $pageHeader->valuesCount() - $nullsCount);

                /** @var array<mixed> $values */
                $values = [];

                foreach ($indices as $index) {
                    $values[] = (($nullsafeVariable1 = $dictionary) ? $nullsafeVariable1->values : null)[$index];
                }
            } else {
                $values = [];
            }

            return new ColumnData($column->type(), $column->logicalType(), $repetitions, $definitions, $values);
        }

        throw new RuntimeException('Encoding ' . $pageHeader->encoding()->name . ' not supported');
    }

    public function decodeDataV2(
        string $buffer,
        FlatColumn $column,
        DataPageHeaderV2 $pageHeader,
        ?Dictionary $dictionary = null
    ) : ColumnData {
        $reader = new BinaryBufferReader($buffer, $this->byteOrder);

        $RLEBitPackedHybrid = new RLEBitPackedHybrid();

        if ($column->maxRepetitionsLevel()) {
            $repetitions = $this->readRLEBitPackedHybrid($reader, $RLEBitPackedHybrid, BitWidth::calculate($column->maxRepetitionsLevel()), $pageHeader->valuesCount());
        } else {
            $repetitions = [];
        }

        if ($column->maxDefinitionsLevel()) {
            $definitions = $this->readRLEBitPackedHybrid($reader, $RLEBitPackedHybrid, BitWidth::calculate($column->maxDefinitionsLevel()), $pageHeader->valuesCount());
        } else {
            $definitions = [];
        }

        $nullsCount = \count($definitions) ? \count(\array_filter($definitions, static function ($definition) {
            return $definition === 0;
        })) : 0;

        if ($pageHeader->encoding() === Encodings::PLAIN) {
            return new ColumnData(
                $column->type(),
                $column->logicalType(),
                $repetitions,
                $definitions,
                (new PlainValueUnpacker($reader))->unpack($column, $pageHeader->valuesCount() - $nullsCount)
            );
        }

        if ($pageHeader->encoding() === Encodings::RLE_DICTIONARY || $pageHeader->encoding() === Encodings::PLAIN_DICTIONARY) {
            if (\count($definitions)) {
                // while reading indices, there is no length at the beginning since length is simply a remaining length of the buffer
                // however we need to know bitWidth which is the first value in the buffer after definitions
                $bitWidth = $reader->readBytes(1)->toInt();
                /** @var array<int> $indices */
                $indices = $this->readRLEBitPackedHybrid($reader, $RLEBitPackedHybrid, $bitWidth, $pageHeader->valuesCount() - $nullsCount);

                /** @var array<mixed> $values */
                $values = [];

                foreach ($indices as $index) {
                    $values[] = (($nullsafeVariable2 = $dictionary) ? $nullsafeVariable2->values : null)[$index];
                }
            } else {
                $values = [];
            }

            return new ColumnData($column->type(), $column->logicalType(), $repetitions, $definitions, $values);
        }

        throw new RuntimeException('Encoding ' . $pageHeader->encoding()->name . ' not supported');
    }

    public function decodeDictionary(
        string $buffer,
        FlatColumn $column,
        DictionaryPageHeader $pageHeader
    ) : Dictionary {
        $reader = new BinaryBufferReader($buffer, $this->byteOrder);

        return new Dictionary(
            (new PlainValueUnpacker($reader))->unpack($column, $pageHeader->valuesCount())
        );
    }

    private function readRLEBitPackedHybrid(BinaryBufferReader $reader, RLEBitPackedHybrid $RLEBitPackedHybrid, int $bitWidth, int $expectedValuesCount) : array
    {
        return $RLEBitPackedHybrid->decodeHybrid($reader, $bitWidth, $expectedValuesCount);
    }
}
