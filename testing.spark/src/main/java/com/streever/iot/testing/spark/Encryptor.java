package com.streever.iot.testing.spark;

import javax.crypto.*;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import static javax.crypto.Cipher.ENCRYPT_MODE;

public class Encryptor implements Serializable {
    private static final long serialVersionUID = -3268111952364680756L;

    private static final String ASCII_CHARSET = "US-ASCII";
    private static final String KEY_CIPHER = "AES/ECB/NoPadding";
    private static final int MASK_7BIT = 0x7F;
    private static final int BITS_IN_ASCII = 7;
    private static final int BITS_IN_BYTE = 8;
    private static final int ASCII_IN_LONG = (Long.BYTES * BITS_IN_BYTE) / BITS_IN_ASCII;
    private static final int KEY_LENGTH = 17;
    private static final int LONGS_IN_KEY = (KEY_LENGTH + ASCII_IN_LONG - 1) / ASCII_IN_LONG;
    private static final int BYTES_IN_KEY = LONGS_IN_KEY * Long.BYTES;

    private final SecretKey key;

    public Encryptor(final SecretKey key) {
        this.key = key;
    }

    public String encrypt(final String lclKey)
            throws BadPaddingException, IllegalBlockSizeException, NoSuchPaddingException,
            NoSuchAlgorithmException, InvalidKeyException, UnsupportedEncodingException {

        final Cipher cipher = Cipher.getInstance(KEY_CIPHER);
        cipher.init(ENCRYPT_MODE, this.key);
        final byte[] plaintext = compressKey(lclKey);
        final byte[] ciphertext = cipher.doFinal(plaintext);
        return Base64.getEncoder().encodeToString(ciphertext);
    }

    private static byte[] compressKey(final String lclKey)
            throws UnsupportedEncodingException {

        final byte[] chars = lclKey.getBytes(ASCII_CHARSET);
        final ByteBuffer byteBuffer = ByteBuffer.allocate(BYTES_IN_KEY);

        for (int i = 0, k = 0; i < LONGS_IN_KEY; ++i, k += ASCII_IN_LONG) {
            long tmp = 0;
            for (int j = k; j < k + ASCII_IN_LONG && j < chars.length; ++j) {
                tmp <<= BITS_IN_ASCII;
                tmp |= (chars[j] & MASK_7BIT);
            }
            byteBuffer.putLong(tmp);
        }
        return byteBuffer.array();
    }

}
