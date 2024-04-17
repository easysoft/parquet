<?php

declare(strict_types=1);

namespace Flow\Parquet;

final class DataSize
{
    /**
     * @var int
     */
    private $bits;
    /**
     * @var int|null
     */
    private $bytes;

    public function __construct(int $bits)
    {
        $this->bits = $bits;
    }

    public static function fromBytes(int $bytes) : self
    {
        return new self($bytes * 8);
    }

    /**
     * @param int|$this $bits
     */
    public function add($bits) : void
    {
        if ($bits instanceof self) {
            $this->bits += $bits->bits;
            $this->bytes = (int) \round($this->bits / 8, 0, PHP_ROUND_HALF_DOWN);

            return;
        }

        $this->bits += $bits;
        $this->bytes = (int) \round($this->bits / 8, 0, PHP_ROUND_HALF_DOWN);
    }

    public function addBytes(int $bytes) : void
    {
        $this->add($bytes * 8);
    }

    public function bits() : int
    {
        return $this->bits;
    }

    public function bytes() : int
    {
        if ($this->bytes === null) {
            $this->bytes = (int) \round($this->bits / 8, 0, PHP_ROUND_HALF_DOWN);
        }

        return $this->bytes;
    }

    /**
     * @param int|$this $bits
     */
    public function sub($bits) : void
    {
        if ($bits instanceof self) {
            $this->bits -= $bits->bits;
            $this->bytes = (int) \round($this->bits / 8, 0, PHP_ROUND_HALF_DOWN);

            return;
        }

        $this->bits -= $bits;
        $this->bytes = (int) \round($this->bits / 8, 0, PHP_ROUND_HALF_DOWN);
    }

    public function subBytes(int $bytes) : void
    {
        $this->sub($bytes * 8);
    }
}
