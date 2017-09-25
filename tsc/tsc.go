// Package tsc implement time-series compression
package tsc

import (
	"github.com/huangaz/tsc/bitUtil"
	"math"
)

const (
	DEFAULT_DELTA             = 60
	BITS_FOR_FIRST_TIMESTAMP  = 32
	LEADING_ZEROS_LENGTH_BITS = 5
	BLOCK_SIZE_LENGTH_BITS    = 6
	BLOCK_SIZE_ADJUSTMENT     = 1
	MAX_LEADING_ZEROS_LENGTH  = (1 << LEADING_ZEROS_LENGTH_BITS) - 1
)

type Series struct {
	Bs bitUtil.BitStream

	prevTimeWrite      uint64
	prevTimeDeltaWrite int64

	prevTimeRead      uint64
	prevTimeDeltaRead int64

	prevValueWrite    float64
	prevLeadingWrite  uint64
	prevTrailingWrite uint64

	prevValueRead    float64
	prevLeadingRead  uint64
	prevTrailingRead uint64
}

type timestampEncoding struct {
	bitsForvalue          uint64
	controlValue          uint64
	controlValueBitLength uint64
}

var timestampEncodings = []timestampEncoding{
	{7, 2, 2},
	{9, 6, 3},
	{12, 14, 4},
	{32, 15, 4},
}

func (s *Series) Append(timestamp uint64, value float64) {
	s.appendTimestamp(timestamp)
	s.appendValue(value)
}

func (s *Series) Read() (uint64, float64, error) {
	timestamp, err := s.readNextTimestamp()
	if err != nil {
		return 0, 0, err
	}
	value, err := s.readNextValue()
	if err != nil {
		return 0, 0, err
	}
	return timestamp, value, nil
}

// timestamp:0-4294967295
func (s *Series) appendTimestamp(timestamp uint64) {
	if len(s.Bs.Stream) == 0 {
		//store the first timestamp
		s.Bs.AddValueToBitStream(timestamp, uint64(BITS_FOR_FIRST_TIMESTAMP))
		s.prevTimeWrite = timestamp
		s.prevTimeDeltaWrite = DEFAULT_DELTA
		return
	}

	delta := int64(timestamp - s.prevTimeWrite)
	deltaOfDelta := delta - s.prevTimeDeltaWrite

	if deltaOfDelta == 0 {
		s.prevTimeWrite = timestamp
		s.Bs.AddValueToBitStream(uint64(0), uint64(1))
		return
	}

	if deltaOfDelta > 0 {
		// There are no zeros. Shift by one to fit in x number of bits
		deltaOfDelta--
	}

	absValue := int64(math.Abs(float64(deltaOfDelta)))
	for i := 0; i < 4; i++ {
		if absValue < (1 << uint(timestampEncodings[i].bitsForvalue-1)) {
			s.Bs.AddValueToBitStream(timestampEncodings[i].controlValue, timestampEncodings[i].controlValueBitLength)
			// Make this value between [0, 2^timestampEncodings[i].bitsForvalue - 1]
			encodedValue := uint64(deltaOfDelta + (1 << uint(timestampEncodings[i].bitsForvalue-1)))
			s.Bs.AddValueToBitStream(encodedValue, timestampEncodings[i].bitsForvalue)
			break
		}
	}

	s.prevTimeWrite = timestamp
	s.prevTimeDeltaWrite = delta
}

func (s *Series) readNextTimestamp() (uint64, error) {
	if s.Bs.BitPos == 0 {
		s.prevTimeDeltaRead = DEFAULT_DELTA
		if res, err := s.Bs.ReadValueFromBitStream(BITS_FOR_FIRST_TIMESTAMP); err != nil {
			return 0, err
		} else {
			s.prevTimeRead = res
			return res, nil
		}
	}

	index, err := s.Bs.FindTheFirstZerobit(4)
	if err != nil {
		return 0, err
	}
	if index > 0 {
		// Delta of delta is non zero. Calculate the new delta.
		// 'index' will be used to find the right length for the value
		// that is read.
		index--
		decodeValue, err := s.Bs.ReadValueFromBitStream(timestampEncodings[index].bitsForvalue)
		if err != nil {
			return 0, err
		}
		value := int64(decodeValue)
		// [0,255] becomes [-128,127]
		value -= (1 << (timestampEncodings[index].bitsForvalue - 1))
		if value >= 0 {
			// [-128,127] becomes [-128,128] without the zero in the middle
			value++
		}
		s.prevTimeDeltaRead += value
	}
	s.prevTimeRead += uint64(s.prevTimeDeltaRead)
	return s.prevTimeRead, nil
}

func (s *Series) appendValue(value float64) {
	xorWithprev := math.Float64bits(value) ^ math.Float64bits(s.prevValueWrite)
	if xorWithprev == 0 {
		s.Bs.AddValueToBitStream(0, 1)
		return
	} else {
		s.Bs.AddValueToBitStream(1, 1)
	}

	leading := bitUtil.Clz(xorWithprev)
	trailing := bitUtil.Ctz(xorWithprev)

	if leading > MAX_LEADING_ZEROS_LENGTH {
		leading = MAX_LEADING_ZEROS_LENGTH
	}

	blockSize := 64 - leading - trailing
	expectedSize := LEADING_ZEROS_LENGTH_BITS + BLOCK_SIZE_LENGTH_BITS + blockSize
	prevBolckInformationSize := 64 - s.prevLeadingWrite - s.prevTrailingWrite

	if leading >= s.prevLeadingWrite && trailing >= s.prevTrailingWrite && prevBolckInformationSize < expectedSize {
		//Control bit for using previous block information.
		s.Bs.AddValueToBitStream(1, 1)
		blockValue := xorWithprev >> s.prevTrailingWrite
		s.Bs.AddValueToBitStream(blockValue, prevBolckInformationSize)
	} else {
		//Control bit for not using previous block information.
		s.Bs.AddValueToBitStream(0, 1)
		s.Bs.AddValueToBitStream(leading, LEADING_ZEROS_LENGTH_BITS)
		//To fit in 6 bits. There will never be a zero size block
		s.Bs.AddValueToBitStream(blockSize-BLOCK_SIZE_ADJUSTMENT, BLOCK_SIZE_LENGTH_BITS)
		blockValue := xorWithprev >> trailing
		s.Bs.AddValueToBitStream(blockValue, blockSize)
		s.prevLeadingWrite = leading
		s.prevTrailingWrite = trailing
	}
	s.prevValueWrite = value
}

func (s *Series) readNextValue() (float64, error) {
	nonZeroValue, err := s.Bs.ReadValueFromBitStream(1)
	if err != nil {
		return 0, err
	}

	if nonZeroValue == 0 {
		return s.prevValueRead, nil
	}

	usepreviousBlockInformation, err := s.Bs.ReadValueFromBitStream(1)
	if err != nil {
		return 0, err
	}

	var xorValue uint64
	if usepreviousBlockInformation == 1 {
		xorValue, err = s.Bs.ReadValueFromBitStream(64 - s.prevLeadingRead - s.prevTrailingRead)
		if err != nil {
			return 0, err
		}
		xorValue <<= s.prevTrailingRead
	} else {
		leading, err := s.Bs.ReadValueFromBitStream(LEADING_ZEROS_LENGTH_BITS)
		if err != nil {
			return 0, err
		}
		blockSize, err := s.Bs.ReadValueFromBitStream(BLOCK_SIZE_LENGTH_BITS)
		if err != nil {
			return 0, err
		}
		blockSize += BLOCK_SIZE_ADJUSTMENT
		s.prevTrailingRead = 64 - leading - blockSize
		xorValue, err = s.Bs.ReadValueFromBitStream(blockSize)
		if err != nil {
			return 0, err
		}
		xorValue <<= s.prevTrailingRead
		s.prevLeadingRead = leading
	}

	value := math.Float64frombits(xorValue ^ math.Float64bits(s.prevValueRead))
	s.prevValueRead = value
	return value, nil
}
