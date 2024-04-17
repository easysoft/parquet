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

class AesGcmCtrV1 extends TBase
{
    public static $_TSPEC = [
        1 => [
            'var' => 'aad_prefix',
            'isRequired' => false,
            'type' => TType::STRING,
        ],
        2 => [
            'var' => 'aad_file_unique',
            'isRequired' => false,
            'type' => TType::STRING,
        ],
        3 => [
            'var' => 'supply_aad_prefix',
            'isRequired' => false,
            'type' => TType::BOOL,
        ],
    ];

    public static $isValidate = false;

    /**
     * Unique file identifier part of AAD suffix *.
     *
     * @var string
     */
    public $aad_file_unique;

    /**
     * AAD prefix *.
     *
     * @var string
     */
    public $aad_prefix;

    /**
     * In files encrypted with AAD prefix without storing it,
     * readers must supply the prefix *.
     *
     * @var bool
     */
    public $supply_aad_prefix;

    public function __construct($vals = null)
    {
        if (\is_array($vals)) {
            parent::__construct(self::$_TSPEC, $vals);
        }
    }

    public function getName()
    {
        return 'AesGcmCtrV1';
    }

    public function read($input)
    {
        return $this->_read('AesGcmCtrV1', self::$_TSPEC, $input);
    }

    public function write($output)
    {
        return $this->_write('AesGcmCtrV1', self::$_TSPEC, $output);
    }
}
