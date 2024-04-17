<?php

declare(strict_types=1);
namespace Flow\Parquet\Thrift;

/**
 * Autogenerated by Thrift Compiler (0.19.0).
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *
 *  @generated
 */
use Thrift\Base\TBase;
use Thrift\Type\TType;

class ColumnCryptoMetaData extends TBase
{
    public static $_TSPEC = [
        1 => [
            'var' => 'ENCRYPTION_WITH_FOOTER_KEY',
            'isRequired' => false,
            'type' => TType::STRUCT,
            'class' => '\Flow\Parquet\Thrift\EncryptionWithFooterKey',
        ],
        2 => [
            'var' => 'ENCRYPTION_WITH_COLUMN_KEY',
            'isRequired' => false,
            'type' => TType::STRUCT,
            'class' => '\Flow\Parquet\Thrift\EncryptionWithColumnKey',
        ],
    ];

    public static $isValidate = false;

    /**
     * @var EncryptionWithColumnKey
     */
    public $ENCRYPTION_WITH_COLUMN_KEY;

    /**
     * @var EncryptionWithFooterKey
     */
    public $ENCRYPTION_WITH_FOOTER_KEY;

    public function __construct($vals = null)
    {
        if (\is_array($vals)) {
            parent::__construct(self::$_TSPEC, $vals);
        }
    }

    public function getName()
    {
        return 'ColumnCryptoMetaData';
    }

    public function read($input)
    {
        return $this->_read('ColumnCryptoMetaData', self::$_TSPEC, $input);
    }

    public function write($output)
    {
        return $this->_write('ColumnCryptoMetaData', self::$_TSPEC, $output);
    }
}