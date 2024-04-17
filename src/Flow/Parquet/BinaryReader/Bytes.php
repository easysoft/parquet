<?php

declare(strict_types=1);

namespace Flow\Parquet\BinaryReader;

use Flow\Parquet\{ByteOrder, DataSize};

final class Bytes implements \ArrayAccess, \Countable, \IteratorAggregate
{
    /**
     * @var mixed[]
     */
    private $bytes;
    /**
     * @readonly
     * @var \Flow\Parquet\ByteOrder
     */
    private $byteOrder = ByteOrder::LITTLE_ENDIAN;
    /**
     * @var \ArrayIterator|null
     */
    private $iterator;

    /**
     * @readonly
     * @var \Flow\Parquet\DataSize
     */
    private $size;

    /**
     * @param \Flow\Parquet\ByteOrder::* $byteOrder
     */
    public function __construct(
        array $bytes,
        string $byteOrder = ByteOrder::LITTLE_ENDIAN
    ) {
        $this->bytes = $bytes;
        $this->byteOrder = $byteOrder;
        $this->size = new DataSize(\count($this->bytes) * 8);
    }

    /**
     * @param \Flow\Parquet\ByteOrder::* $byteOrder
     */
    public static function fromString(string $string, string $byteOrder = ByteOrder::LITTLE_ENDIAN) : self
    {
        /** @phpstan-ignore-next-line */
        return new self(\array_values(\unpack('C*', $string)), $byteOrder);
    }

    // Countable methods
    public function count() : int
    {
        return \count($this->bytes);
    }

    // IteratorAggregate methods
    public function getIterator() : \ArrayIterator
    {
        if ($this->iterator === null) {
            $this->iterator = new \ArrayIterator($this->bytes);
        }

        return $this->iterator;
    }

    // ArrayAccess methods
    public function offsetExists($offset) : bool
    {
        return isset($this->bytes[$offset]);
    }

    /**
     * @return mixed
     */
    #[\ReturnTypeWillChange]
    public function offsetGet($offset)
    {
        return $this->bytes[$offset];
    }

    public function offsetSet($offset, $value) : void
    {
        if ($offset === null) {
            $this->bytes[] = $value;
        } else {
            $this->bytes[$offset] = $value;
        }
    }

    public function offsetUnset($offset) : void
    {
        unset($this->bytes[$offset]);
    }

    public function size() : DataSize
    {
        return $this->size;
    }

    /**
     * @return array<int>
     */
    public function toArray() : array
    {
        return $this->bytes;
    }

    /**
     * Convert bytes to a single integer.
     */
    public function toInt() : int
    {
        $result = 0;
        $bytes = $this->byteOrder === ByteOrder::LITTLE_ENDIAN ? $this->bytes : \array_reverse($this->bytes);

        foreach ($bytes as $shift => $byte) {
            $result |= ($byte << ($shift * 8));
        }

        return $result;
    }

    public function toString() : string
    {
        return \pack('C*', ...$this->bytes);
    }
}
