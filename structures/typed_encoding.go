package structures

import (
	"encoding/binary"
	"errors"
	"unsafe"
)

// Typed binary encoding for hot-path structs (DESIGN notes: msgpack
// baseline is ~12 allocs per encode/decode cycle for AddrSCIDEntry; a
// hand-rolled typed encoder should drop that to 0 on encode and ~1 on
// decode).
//
// Format layout for AddrSCIDEntry:
//
//   +------+------+------+------+------+------+------+------+------+
//   | 0x01 |  FirstHeight (BE 8 bytes)                              |
//   +------+------+------+------+------+------+------+------+------+
//   |  LastHeight (BE 8 bytes)                                      |
//   +------+------+------+------+------+------+------+------+------+
//   |  Count (BE 8 bytes)                                           |
//   +------+------+------+------+------+------+------+------+------+
//
// Total: 25 bytes. First byte is a format tag:
//   - 0x01: typed v1 (this format)
//   - 0x80..0x8f: msgpack fixmap (legacy; we'd fall through to msgpack
//     decode if we see one).
//
// The tag never collides with any msgpack encoding of a map-valued struct
// because msgpack's fixmap family starts at 0x80.

const (
	// TagAddrSCIDEntryV1 marks the typed v1 encoding of AddrSCIDEntry.
	TagAddrSCIDEntryV1 byte = 0x01

	// EncodedAddrSCIDEntrySize is the fixed byte length of a v1 record.
	EncodedAddrSCIDEntrySize = 1 + 8 + 8 + 8 // tag + 3× int64
)

// ErrInvalidAddrSCIDEntry is returned when the wire bytes don't match
// either the legacy msgpack shape or the v1 typed shape.
var ErrInvalidAddrSCIDEntry = errors.New("invalid AddrSCIDEntry encoding")

// MarshalTyped encodes the entry into a fresh 25-byte slice. The caller
// can hand the slice straight to bbolt.Put (which copies internally).
func (e *AddrSCIDEntry) MarshalTyped() []byte {
	buf := make([]byte, EncodedAddrSCIDEntrySize)
	buf[0] = TagAddrSCIDEntryV1
	binary.BigEndian.PutUint64(buf[1:9], int64Bits(e.FirstHeight))
	binary.BigEndian.PutUint64(buf[9:17], int64Bits(e.LastHeight))
	binary.BigEndian.PutUint64(buf[17:25], int64Bits(e.Count))
	return buf
}

// MarshalTypedAppend writes into an existing buffer (returning the grown
// slice). Intended for keyBuf-style reuse in FlushBatch — eliminates the
// per-call allocation when called in a tight loop.
func (e *AddrSCIDEntry) MarshalTypedAppend(dst []byte) []byte {
	// Grow once if needed; append semantics preserve existing prefix if caller
	// wants to pack multiple records (they currently don't, but the API is
	// allocation-flexible).
	dst = append(dst, TagAddrSCIDEntryV1)
	dst = binary.BigEndian.AppendUint64(dst, int64Bits(e.FirstHeight))
	dst = binary.BigEndian.AppendUint64(dst, int64Bits(e.LastHeight))
	dst = binary.BigEndian.AppendUint64(dst, int64Bits(e.Count))
	return dst
}

// UnmarshalTyped decodes a v1 typed entry from b. Returns
// ErrInvalidAddrSCIDEntry if the tag or length doesn't match.
func (e *AddrSCIDEntry) UnmarshalTyped(b []byte) error {
	if len(b) != EncodedAddrSCIDEntrySize || b[0] != TagAddrSCIDEntryV1 {
		return ErrInvalidAddrSCIDEntry
	}
	e.FirstHeight = int64FromBits(binary.BigEndian.Uint64(b[1:9]))
	e.LastHeight = int64FromBits(binary.BigEndian.Uint64(b[9:17]))
	e.Count = int64FromBits(binary.BigEndian.Uint64(b[17:25]))
	return nil
}

// IsAddrSCIDEntryTyped reports whether b is v1-typed (as opposed to the
// legacy msgpack encoding). msgpack fixmap headers live in 0x80-0x8f;
// v1 uses 0x01 so the check is a single byte comparison.
func IsAddrSCIDEntryTyped(b []byte) bool {
	return len(b) >= 1 && b[0] == TagAddrSCIDEntryV1
}

// ============================================================================
// InstallRecord — typed v1 encoding
// ============================================================================
//
// Wire layout:
//
//   byte 0        : tag 0x05
//   bytes 1..8    : Fees (BE uint64)
//   varint+bytes  : Owner
//   varint+bytes  : Entrypoint

const (
	TagInstallRecordV1 byte = 0x05

	installRecordMinHeaderSize = 1 + 8
)

var ErrInvalidInstallRecord = errors.New("invalid InstallRecord encoding")

func (r *InstallRecord) MarshalTyped() []byte {
	buf := make([]byte, 0, installRecordMinHeaderSize+2+len(r.Owner)+len(r.Entrypoint))
	return r.MarshalTypedAppend(buf)
}

func (r *InstallRecord) MarshalTypedAppend(dst []byte) []byte {
	dst = append(dst, TagInstallRecordV1)
	dst = binary.BigEndian.AppendUint64(dst, r.Fees)
	dst = appendLenString(dst, r.Owner)
	dst = appendLenString(dst, r.Entrypoint)
	return dst
}

func (r *InstallRecord) UnmarshalTyped(b []byte) error {
	if len(b) < installRecordMinHeaderSize || b[0] != TagInstallRecordV1 {
		return ErrInvalidInstallRecord
	}
	r.Fees = binary.BigEndian.Uint64(b[1:9])
	pos := 9
	var err error
	if r.Owner, pos, err = readLenString(b, pos); err != nil {
		return ErrInvalidInstallRecord
	}
	if r.Entrypoint, pos, err = readLenString(b, pos); err != nil {
		return ErrInvalidInstallRecord
	}
	_ = pos
	return nil
}

func IsInstallRecordTyped(b []byte) bool {
	return len(b) >= 1 && b[0] == TagInstallRecordV1
}

// ============================================================================
// NormalTXWithSCIDParse — typed v1 encoding
// ============================================================================
//
// Wire layout:
//
//   byte 0        : tag 0x08
//   bytes 1..8    : Fees   (BE uint64)
//   bytes 9..16   : Height (BE int64)
//   varint+bytes  : Txid
//   varint+bytes  : Scid
//
// Backward compat: legacy records are msgpack — a single-record struct
// (fixmap 0x84) under a composite key, or a []txs blob (fixarray 0x9X) under
// a bare addr key. Tag 0x08 is disjoint from both, so reader dispatch by
// byte[0] is unambiguous. See IsNormalTxTyped.

const (
	TagNormalTxV1 byte = 0x08

	normalTxMinHeaderSize = 1 + 8 + 8 // tag + Fees + Height
)

var ErrInvalidNormalTx = errors.New("invalid NormalTXWithSCIDParse encoding")

func (r *NormalTXWithSCIDParse) MarshalTyped() []byte {
	buf := make([]byte, 0, normalTxMinHeaderSize+2+len(r.Txid)+len(r.Scid))
	return r.MarshalTypedAppend(buf)
}

func (r *NormalTXWithSCIDParse) MarshalTypedAppend(dst []byte) []byte {
	dst = append(dst, TagNormalTxV1)
	dst = binary.BigEndian.AppendUint64(dst, r.Fees)
	dst = binary.BigEndian.AppendUint64(dst, int64Bits(r.Height))
	dst = appendLenString(dst, r.Txid)
	dst = appendLenString(dst, r.Scid)
	return dst
}

func (r *NormalTXWithSCIDParse) UnmarshalTyped(b []byte) error {
	if len(b) < normalTxMinHeaderSize || b[0] != TagNormalTxV1 {
		return ErrInvalidNormalTx
	}
	r.Fees = binary.BigEndian.Uint64(b[1:9])
	r.Height = int64FromBits(binary.BigEndian.Uint64(b[9:17]))
	pos := 17
	var err error
	if r.Txid, pos, err = readLenString(b, pos); err != nil {
		return ErrInvalidNormalTx
	}
	if r.Scid, pos, err = readLenString(b, pos); err != nil {
		return ErrInvalidNormalTx
	}
	_ = pos
	return nil
}

func IsNormalTxTyped(b []byte) bool {
	return len(b) >= 1 && b[0] == TagNormalTxV1
}

// ============================================================================
// SCTXParse turbo subset — typed v1 encoding
// ============================================================================
//
// Turbo-mode scan records do not persist ScArgs or Payloads. This compact
// record stores the fields queried by the existing API without msgpack
// reflection.

const (
	TagSCTXParseTurboV1 byte = 0x06

	sctxParseTurboMinHeaderSize = 1 + 1 + 8 + 8 // tag + method + fees + height
)

var ErrInvalidSCTXParseTurbo = errors.New("invalid SCTXParse turbo encoding")

func (s *SCTXParse) CanMarshalTurboTyped() bool {
	return s != nil && len(s.ScArgs) == 0 && len(s.Payloads) == 0
}

func (s *SCTXParse) MarshalTurboTyped() []byte {
	buf := make([]byte, 0, sctxParseTurboMinHeaderSize+
		len(s.Txid)+len(s.Scid)+len(s.Entrypoint)+len(s.Sender)+4)
	return s.MarshalTurboTypedAppend(buf)
}

func (s *SCTXParse) MarshalTurboTypedAppend(dst []byte) []byte {
	dst = append(dst, TagSCTXParseTurboV1)
	dst = append(dst, s.Method)
	dst = binary.BigEndian.AppendUint64(dst, s.Fees)
	dst = binary.BigEndian.AppendUint64(dst, int64Bits(s.Height))
	dst = appendLenString(dst, s.Txid)
	dst = appendLenString(dst, s.Scid)
	dst = appendLenString(dst, s.Entrypoint)
	dst = appendLenString(dst, s.Sender)
	return dst
}

func (s *SCTXParse) UnmarshalTurboTyped(b []byte) error {
	if len(b) < sctxParseTurboMinHeaderSize || b[0] != TagSCTXParseTurboV1 {
		return ErrInvalidSCTXParseTurbo
	}
	s.Method = b[1]
	s.Fees = binary.BigEndian.Uint64(b[2:10])
	s.Height = int64FromBits(binary.BigEndian.Uint64(b[10:18]))

	// Locate the four length-prefixed string ranges without allocating, then
	// copy them all into ONE owned backing buffer. The previous code did four
	// separate string(b[...]) copies (4 allocs); this is a single alloc. The
	// returned strings view sbuf — freshly allocated, never mutated after the
	// copies, and outliving both b and the strings themselves — so they do NOT
	// alias the caller's b (which a bbolt View frees after the read returns).
	// Guarded by TestLoop_NoAlias_SCTXParseTurbo.
	pos := 18
	var rs, re [4]int
	for i := 0; i < 4; i++ {
		st, en, nx, err := locLenString(b, pos)
		if err != nil {
			return ErrInvalidSCTXParseTurbo
		}
		rs[i], re[i] = st, en
		pos = nx
	}
	total := (re[0] - rs[0]) + (re[1] - rs[1]) + (re[2] - rs[2]) + (re[3] - rs[3])
	var sbuf []byte
	if total > 0 {
		sbuf = make([]byte, total)
	}
	off := 0
	s.Txid = ownedCut(sbuf, &off, b, rs[0], re[0])
	s.Scid = ownedCut(sbuf, &off, b, rs[1], re[1])
	s.Entrypoint = ownedCut(sbuf, &off, b, rs[2], re[2])
	s.Sender = ownedCut(sbuf, &off, b, rs[3], re[3])
	s.ScArgs = nil
	s.Payloads = nil
	return nil
}

// ownedCut copies b[start:end] into sbuf at *off (advancing it) and returns a
// string viewing that owned region. Empty ranges return "" without touching
// sbuf. The caller guarantees sbuf has room for every cut and never mutates it
// afterward, so the returned strings are safe, non-aliasing views into memory
// the decoder owns. Shared by the turbo/typed decoders that coalesce their
// string fields into a single backing allocation.
func ownedCut(sbuf []byte, off *int, b []byte, start, end int) string {
	n := end - start
	if n == 0 {
		return ""
	}
	copy(sbuf[*off:*off+n], b[start:end])
	out := unsafe.String(&sbuf[*off], n)
	*off += n
	return out
}

// locLenString returns the [start,end) byte range of a uvarint-length-prefixed
// string at pos, plus the next read position, WITHOUT allocating the string.
// Companion to readLenString for decoders that coalesce their string fields.
func locLenString(b []byte, pos int) (start, end, next int, err error) {
	if pos >= len(b) {
		return 0, 0, pos, ErrInvalidClassMeta
	}
	n, nLen := binary.Uvarint(b[pos:])
	if nLen <= 0 {
		return 0, 0, pos, ErrInvalidClassMeta
	}
	pos += nLen
	if uint64(len(b)-pos) < n { // #nosec G115 -- pos is bounded by prior slice and varint checks.
		return 0, 0, pos, ErrInvalidClassMeta
	}
	start = pos
	end = pos + int(n) // #nosec G115 -- bounded by len(b)-pos above.
	return start, end, end, nil
}

func IsSCTXParseTurboTyped(b []byte) bool {
	return len(b) >= 1 && b[0] == TagSCTXParseTurboV1
}

// ============================================================================
// SCIDVariable slice — typed v1 encoding
// ============================================================================
//
// SCIDVariable holds interface{} Key and Value. In practice only two
// concrete types appear from the DERO SDK: string (DVM-BASIC string var)
// and uint64 (DVM-BASIC numeric var). The typed encoder dispatches on
// these two cases per field.
//
// Wire layout:
//
//   byte 0          : tag 0x02
//   bytes 1..4      : count (BE uint32)
//   for each variable:
//     byte         : key kind (0x01 string, 0x02 uint64)
//     string:      : varint len + raw bytes
//     uint64:      : 8 bytes BE
//     byte         : value kind
//     string/uint64: as above
//
// Backward compat: msgpack array header is 0x90-0x9f (fixarray). Tag
// 0x02 does not overlap, so reader dispatch by byte[0] is unambiguous.

const (
	TagSCIDVariablesV1 byte = 0x02

	varKindString byte = 0x01
	varKindUint64 byte = 0x02
)

// ErrInvalidSCIDVariables is returned when the wire bytes don't match
// either a msgpack array or a v1 typed SCIDVariables slice.
var ErrInvalidSCIDVariables = errors.New("invalid SCIDVariable slice encoding")

// MarshalSCIDVariablesTypedAppend writes the slice into dst in v1 typed
// form. Returns the grown slice. Bytes in dst before the call are
// preserved (so callers can pack multiple values if they want; today
// they use this in a "start empty, fill, hand to Put" pattern).
//
// Interface values that aren't string or uint64 get skipped with a
// distinguishable empty-string placeholder (kind=string, len=0). This
// preserves slice length for downstream consumers that index by position.
func MarshalSCIDVariablesTypedAppend(dst []byte, vars []*SCIDVariable) []byte {
	dst = append(dst, TagSCIDVariablesV1)
	// count (BE uint32)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(vars))) // #nosec G115 -- SC variable snapshots cannot approach uint32 capacity in memory.
	for _, v := range vars {
		dst = appendVarField(dst, v.Key)
		dst = appendVarField(dst, v.Value)
	}
	return dst
}

// MarshalSCIDVariablesTyped encodes vars into a freshly-allocated,
// exactly-sized byte slice. One cheap sizing pass computes the encoded
// length up front, so the append chain never reallocates — unlike the
// MarshalSCIDVariablesTypedAppend(nil, vars) pattern, which pays
// log2(size) grow+copy cycles per snapshot. Wire bytes are identical to
// MarshalSCIDVariablesTypedAppend (it delegates to it).
func MarshalSCIDVariablesTyped(vars []*SCIDVariable) []byte {
	size := 1 + 4 // tag + count (BE uint32)
	for _, v := range vars {
		size += sizeVarField(v.Key) + sizeVarField(v.Value)
	}
	return MarshalSCIDVariablesTypedAppend(make([]byte, 0, size), vars)
}

// sizeVarField returns the exact number of bytes appendVarField will
// write for v. Must stay in lockstep with appendVarField.
func sizeVarField(v interface{}) int {
	switch x := v.(type) {
	case string:
		return 1 + uvarintLen(uint64(len(x))) + len(x)
	case uint64:
		return 1 + 8
	default:
		// Rare fallback path: mirrors appendVarField's stringification.
		// toStringBest may allocate here (and again during encode), but
		// unknown types only appear via filter-bypass SCs, never on the
		// hot string/uint64 path.
		s := toStringBest(v)
		return 1 + uvarintLen(uint64(len(s))) + len(s)
	}
}

// uvarintLen returns the number of bytes binary.AppendUvarint emits for u.
func uvarintLen(u uint64) int {
	n := 1
	for u >= 0x80 {
		u >>= 7
		n++
	}
	return n
}

// UnmarshalSCIDVariablesTyped decodes a v1 typed slice from b.
// Returns a freshly-allocated slice; callers own it.
//
// Arena allocation: all SCIDVariable structs come from one backing array
// (a single make) instead of one heap object per variable. The returned
// []*SCIDVariable still hands out stable pointers — callers never reslice
// or free individual elements, so the shared backing array is invisible
// to them.
func UnmarshalSCIDVariablesTyped(b []byte) ([]*SCIDVariable, error) {
	if len(b) < 5 || b[0] != TagSCIDVariablesV1 {
		return nil, ErrInvalidSCIDVariables
	}
	n := binary.BigEndian.Uint32(b[1:5])
	// Sanity-bound the declared count before allocating: each variable
	// occupies at least 4 wire bytes (2 fields × (kind byte + ≥1 payload
	// byte)), so a count exceeding remaining/4 is corrupt and would only
	// fail later — reject it before the arena make can balloon.
	if uint64(n)*4 > uint64(len(b)-5) { // #nosec G115 -- len(b) >= 5 is guaranteed by the header check above, so the operand is non-negative.
		return nil, ErrInvalidSCIDVariables
	}

	// Pass 1: validate every field and sum the string-field byte lengths so all
	// string payloads share ONE owned backing buffer (one alloc) instead of one
	// string([]byte) copy per string field — the prior hot path cost 11 such
	// copies on the benchmark snapshot. uint64 fields carry no payload bytes.
	// (The interface{} boxing of each Key/Value is structural to SCIDVariable
	// and unavoidable here.) b is immutable between passes. See
	// TestLoop_NoAlias_SCIDVariables.
	total := 0
	p := 5
	for i := uint32(0); i < n; i++ {
		for f := 0; f < 2; f++ {
			kind, st, en, _, nx, err := locVarField(b, p)
			if err != nil {
				return nil, err
			}
			if kind == varKindString {
				total += en - st
			}
			p = nx
		}
	}

	// Pass 2: out + arena (2 allocs) + one backing buffer (1 alloc, if any
	// string bytes), then fill. Strings view the owned buffer; uint64s decode
	// in place.
	out := make([]*SCIDVariable, n)
	arena := make([]SCIDVariable, n)
	var sbuf []byte
	if total > 0 {
		sbuf = make([]byte, total)
	}
	off, pos := 0, 5
	for i := uint32(0); i < n; i++ {
		v := &arena[i]
		kind, st, en, u, nx, err := locVarField(b, pos)
		if err != nil {
			return nil, err
		}
		if kind == varKindString {
			v.Key = ownedCut(sbuf, &off, b, st, en)
		} else {
			v.Key = u
		}
		pos = nx
		kind, st, en, u, nx, err = locVarField(b, pos)
		if err != nil {
			return nil, err
		}
		if kind == varKindString {
			v.Value = ownedCut(sbuf, &off, b, st, en)
		} else {
			v.Value = u
		}
		pos = nx
		out[i] = v
	}
	return out, nil
}

// IsSCIDVariablesTyped reports whether b is the v1 typed encoding.
func IsSCIDVariablesTyped(b []byte) bool {
	return len(b) >= 1 && b[0] == TagSCIDVariablesV1
}

func appendVarField(dst []byte, v interface{}) []byte {
	switch x := v.(type) {
	case string:
		dst = append(dst, varKindString)
		dst = binary.AppendUvarint(dst, uint64(len(x)))
		dst = append(dst, x...)
	case uint64:
		dst = append(dst, varKindUint64)
		dst = binary.BigEndian.AppendUint64(dst, x)
	default:
		// Unknown type — fall back to a stringified form so we preserve
		// *some* data, marked as string. Common case is int64 coming from
		// JSON parsing at upstream; let Sprintf handle it.
		dst = append(dst, varKindString)
		s := toStringBest(v)
		dst = binary.AppendUvarint(dst, uint64(len(s)))
		dst = append(dst, s...)
	}
	return dst
}

// locVarField parses one typed SCIDVariable field at pos WITHOUT allocating.
// For a string field it returns kind=varKindString and the [start,end) byte
// range; for a uint64 field it returns kind=varKindUint64 and the value u.
// next is the position after the field. Wire parsing mirrors the append
// encoder (appendVarField) so the coalescing decoder stays in lockstep with
// the per-field decoder.
func locVarField(b []byte, pos int) (kind byte, start, end int, u uint64, next int, err error) {
	if pos >= len(b) {
		return 0, 0, 0, 0, pos, ErrInvalidSCIDVariables
	}
	kind = b[pos]
	pos++
	switch kind {
	case varKindString:
		n, nLen := binary.Uvarint(b[pos:])
		if nLen <= 0 {
			return 0, 0, 0, 0, pos, ErrInvalidSCIDVariables
		}
		pos += nLen
		if n > uint64(len(b)-pos) { // #nosec G115 -- pos is bounded by prior slice and varint checks.
			return 0, 0, 0, 0, pos, ErrInvalidSCIDVariables
		}
		start = pos
		end = pos + int(n) // #nosec G115 -- bounded by len(b)-pos above.
		return varKindString, start, end, 0, end, nil
	case varKindUint64:
		if pos+8 > len(b) {
			return 0, 0, 0, 0, pos, ErrInvalidSCIDVariables
		}
		return varKindUint64, 0, 0, binary.BigEndian.Uint64(b[pos : pos+8]), pos + 8, nil
	default:
		return 0, 0, 0, 0, pos, ErrInvalidSCIDVariables
	}
}

// toStringBest converts common numeric types to string. Only used for
// unknown-type fallback.
func toStringBest(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case int64:
		return itoaBase10(x)
	case int:
		return itoaBase10(int64(x))
	case float64:
		// Truncate float to int string; SC variables are never true floats.
		return itoaBase10(int64(x))
	default:
		return ""
	}
}

// ============================================================================
// ClassMeta — typed v1 encoding
// ============================================================================
//
// Format layout:
//
//   byte 0           : tag 0x04
//   bytes 1..8       : InstallHeight (BE int64)
//   bytes 9..16      : LastHeight    (BE int64)
//   varint           : len(Tags)
//   for each tag     : varint-len-prefixed bytes
//   varint+bytes     : Class
//   varint+bytes     : Name
//   varint+bytes     : Desc
//   varint+bytes     : IconURL
//   varint+bytes     : DURL
//   varint+bytes     : Version
//
// Optional media tail (written only when at least one member is non-empty):
//
//   varint+bytes     : Image
//   varint+bytes     : AltImage
//   varint+bytes     : Audio
//   varint+bytes     : Video
//   varint+bytes     : ImagesJSON
//
// The tail needs no tag bump in either direction. Forward: the v1 reader
// always ignored trailing bytes (see the `_ = pos` at the end of
// UnmarshalTyped), so a pre-media binary reading a media record decodes the
// six fixed fields and discards the rest. Backward: the reader below treats a
// truncated tail as "absent → empty string" rather than an error, so a media
// binary reads a pre-media record. Omitting the tail entirely when all five
// are empty keeps non-G45 records (TELA, NFA, nameservice) byte-identical to
// what the pre-media encoder produced.
//
// Backward-compat: existing class/class_scid bucket records are msgpack
// fixmaps (tag 0x80-0x8f) or map16 (0xde). Tag 0x04 does not collide with
// either, so reader dispatch by byte[0] is unambiguous. See IsClassMetaTyped.

const (
	TagClassMetaV1 byte = 0x04

	classMetaMinHeaderSize = 1 + 8 + 8 // tag + InstallHeight + LastHeight
)

// ErrInvalidClassMeta is returned when the wire bytes don't match the v1
// typed ClassMeta layout.
var ErrInvalidClassMeta = errors.New("invalid ClassMeta encoding")

// MarshalTyped encodes m into a fresh byte slice. Hand the result to
// bbolt.Put (which must hold the slice until commit — we never reuse).
func (m *ClassMeta) MarshalTyped() []byte {
	// Exact sizing pass so the append chain allocates exactly once — the
	// previous lower-bound estimate omitted the Tags bytes (count varint +
	// each tag's len varint + bytes), so any tagged ClassMeta re-grew the
	// buffer (a second allocation). Mirrors MarshalSCIDVariablesTyped's
	// size-then-fill approach. Stays in lockstep with MarshalTypedAppend.
	size := classMetaMinHeaderSize // tag + InstallHeight + LastHeight
	size += uvarintLen(uint64(len(m.Tags)))
	for _, t := range m.Tags {
		size += uvarintLen(uint64(len(t))) + len(t)
	}
	size += uvarintLen(uint64(len(m.Class))) + len(m.Class)
	size += uvarintLen(uint64(len(m.Name))) + len(m.Name)
	size += uvarintLen(uint64(len(m.Desc))) + len(m.Desc)
	size += uvarintLen(uint64(len(m.IconURL))) + len(m.IconURL)
	size += uvarintLen(uint64(len(m.DURL))) + len(m.DURL)
	size += uvarintLen(uint64(len(m.Version))) + len(m.Version)
	if m.hasMedia() {
		size += uvarintLen(uint64(len(m.Image))) + len(m.Image)
		size += uvarintLen(uint64(len(m.AltImage))) + len(m.AltImage)
		size += uvarintLen(uint64(len(m.Audio))) + len(m.Audio)
		size += uvarintLen(uint64(len(m.Video))) + len(m.Video)
		size += uvarintLen(uint64(len(m.ImagesJSON))) + len(m.ImagesJSON)
	}
	return m.MarshalTypedAppend(make([]byte, 0, size))
}

// hasMedia reports whether the optional media tail must be written. Gating on
// this keeps every non-G45 record byte-identical to the pre-media encoding, so
// existing size benchmarks and encoding goldens do not move.
func (m *ClassMeta) hasMedia() bool {
	return m.Image != "" || m.AltImage != "" || m.Audio != "" ||
		m.Video != "" || m.ImagesJSON != ""
}

// MarshalTypedAppend writes into dst and returns the grown slice.
func (m *ClassMeta) MarshalTypedAppend(dst []byte) []byte {
	dst = append(dst, TagClassMetaV1)
	dst = binary.BigEndian.AppendUint64(dst, int64Bits(m.InstallHeight))
	dst = binary.BigEndian.AppendUint64(dst, int64Bits(m.LastHeight))
	dst = binary.AppendUvarint(dst, uint64(len(m.Tags)))
	for _, t := range m.Tags {
		dst = binary.AppendUvarint(dst, uint64(len(t)))
		dst = append(dst, t...)
	}
	dst = appendLenString(dst, m.Class)
	dst = appendLenString(dst, m.Name)
	dst = appendLenString(dst, m.Desc)
	dst = appendLenString(dst, m.IconURL)
	dst = appendLenString(dst, m.DURL)
	dst = appendLenString(dst, m.Version)
	// Optional media tail — all five or none, so the reader's positional walk
	// stays unambiguous. Stays in lockstep with MarshalTyped's sizing pass.
	if m.hasMedia() {
		dst = appendLenString(dst, m.Image)
		dst = appendLenString(dst, m.AltImage)
		dst = appendLenString(dst, m.Audio)
		dst = appendLenString(dst, m.Video)
		dst = appendLenString(dst, m.ImagesJSON)
	}
	return dst
}

// classMetaMediaFields is the number of optional media strings in the tail.
// Named so the two decode passes cannot drift out of step — a mismatch between
// them under-sizes sbuf and makes ownedCut misalign silently.
const classMetaMediaFields = 5

// UnmarshalTyped decodes a v1 typed ClassMeta from b. Six string fields
// each get one allocation (unavoidable — they must outlive the bbolt View
// that supplied b). Tags slice gets one allocation for its header plus one
// per element string. Typical ClassMeta with 2 tags: 1+2+6 = 9 allocs,
// vs msgpack's 11.
func (m *ClassMeta) UnmarshalTyped(b []byte) error {
	if len(b) < classMetaMinHeaderSize || b[0] != TagClassMetaV1 {
		return ErrInvalidClassMeta
	}
	m.InstallHeight = int64FromBits(binary.BigEndian.Uint64(b[1:9]))
	m.LastHeight = int64FromBits(binary.BigEndian.Uint64(b[9:17]))

	// Pass 1: validate structure and sum the total string bytes (tags + the
	// six string fields) WITHOUT allocating, so pass 2 can copy them all into a
	// single owned backing buffer rather than one string([]byte) alloc per
	// field. b is immutable between passes, so locLenString yields identical
	// ranges both times. See ownedCut / TestLoop_NoAlias_ClassMeta.
	tagCount, nLen := binary.Uvarint(b[17:])
	if nLen <= 0 {
		return ErrInvalidClassMeta
	}
	pos := 17 + nLen
	if tagCount > uint64(len(b)-pos) { // #nosec G115 -- pos is bounded by the fixed header and varint checks.
		return ErrInvalidClassMeta
	}
	total, p := 0, pos
	for i := uint64(0); i < tagCount; i++ {
		st, en, nx, err := locLenString(b, p)
		if err != nil {
			return err
		}
		total += en - st
		p = nx
	}
	for i := 0; i < 6; i++ { // Class, Name, Desc, IconURL, DURL, Version
		st, en, nx, err := locLenString(b, p)
		if err != nil {
			return err
		}
		total += en - st
		p = nx
	}

	// Optional media tail. The writer emits all five or none (see hasMedia), so
	// the read is all-or-nothing: a short or malformed tail is treated as
	// ABSENT rather than an error, which preserves this format's original
	// "trailing bytes are forward-compat slack" contract instead of turning it
	// into a decode failure. mediaOK is the single termination decision both
	// passes obey — b is immutable between them, so pass 2 walking the same
	// bytes reaches the same verdict.
	mediaOK := false
	if p < len(b) {
		q, sum := p, 0
		mediaOK = true
		for i := 0; i < classMetaMediaFields; i++ {
			st, en, nx, err := locLenString(b, q)
			if err != nil {
				mediaOK = false
				break
			}
			sum += en - st
			q = nx
		}
		if mediaOK {
			total += sum
		}
	}

	// Pass 2: at most one tags-slice alloc + one backing-buffer alloc.
	var sbuf []byte
	if total > 0 {
		sbuf = make([]byte, total)
	}
	off := 0
	if tagCount > 0 {
		m.Tags = make([]string, int(tagCount)) // #nosec G115 -- bounded by len(b)-pos above.
		for i := range m.Tags {
			st, en, nx, err := locLenString(b, pos)
			if err != nil {
				return err
			}
			m.Tags[i] = ownedCut(sbuf, &off, b, st, en)
			pos = nx
		}
	} else {
		m.Tags = nil
	}
	var st, en, nx int
	var err error
	if st, en, nx, err = locLenString(b, pos); err != nil {
		return err
	}
	m.Class, pos = ownedCut(sbuf, &off, b, st, en), nx
	if st, en, nx, err = locLenString(b, pos); err != nil {
		return err
	}
	m.Name, pos = ownedCut(sbuf, &off, b, st, en), nx
	if st, en, nx, err = locLenString(b, pos); err != nil {
		return err
	}
	m.Desc, pos = ownedCut(sbuf, &off, b, st, en), nx
	if st, en, nx, err = locLenString(b, pos); err != nil {
		return err
	}
	m.IconURL, pos = ownedCut(sbuf, &off, b, st, en), nx
	if st, en, nx, err = locLenString(b, pos); err != nil {
		return err
	}
	m.DURL, pos = ownedCut(sbuf, &off, b, st, en), nx
	if st, en, nx, err = locLenString(b, pos); err != nil {
		return err
	}
	m.Version, pos = ownedCut(sbuf, &off, b, st, en), nx

	// Media tail, gated on pass 1's verdict. Always assigned — including the
	// clear on the absent path — so decoding a pre-media record into a reused
	// ClassMeta cannot leave a previous record's media behind.
	m.Image, m.AltImage, m.Audio, m.Video, m.ImagesJSON = "", "", "", "", ""
	if mediaOK {
		for _, dst := range [classMetaMediaFields]*string{
			&m.Image, &m.AltImage, &m.Audio, &m.Video, &m.ImagesJSON,
		} {
			if st, en, nx, err = locLenString(b, pos); err != nil {
				return err
			}
			*dst, pos = ownedCut(sbuf, &off, b, st, en), nx
		}
	}
	_ = pos // trailing bytes (forward-compat slack) are ignored
	return nil
}

// IsClassMetaTyped reports whether b is a v1 typed record. The msgpack
// fallback range (0x80-0x8f fixmap, 0xde map16) is disjoint from 0x04.
func IsClassMetaTyped(b []byte) bool {
	return len(b) >= 1 && b[0] == TagClassMetaV1
}

// appendLenString writes a uvarint-prefixed string into dst.
func appendLenString(dst []byte, s string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

// readLenString reads a uvarint-prefixed string starting at pos. Returns
// the string, next position, and any error. Shared helper for ClassMeta
// and any future typed struct that needs the same primitive.
func readLenString(b []byte, pos int) (string, int, error) {
	if pos >= len(b) {
		return "", pos, ErrInvalidClassMeta
	}
	n, nLen := binary.Uvarint(b[pos:])
	if nLen <= 0 {
		return "", pos, ErrInvalidClassMeta
	}
	pos += nLen
	remaining := uint64(len(b) - pos) // #nosec G115 -- pos is bounded by prior slice and varint checks.
	if n > remaining {
		return "", pos, ErrInvalidClassMeta
	}
	end := pos + int(n) // #nosec G115 -- bounded by len(b)-pos above.
	if end > len(b) {
		return "", pos, ErrInvalidClassMeta
	}
	return string(b[pos:end]), end, nil
}

// ============================================================================
// SCCodeEntry — typed v1 encoding
// ============================================================================
//
// Code is the dominant payload; stash it raw after a fixed 9-byte prefix so
// decode is one string allocation (unavoidable — the string must outlive the
// bbolt View closure) and encode is a single append-chain, no allocation
// beyond the eventual grow.
//
// Wire layout:
//
//   +------+------+------+------+------+------+------+------+------+--------+
//   | 0x03 |  InstallHeight (BE 8 bytes)                            | code … |
//   +------+------+------+------+------+------+------+------+------+--------+
//
// Backward compat: no legacy records — sccode is a brand-new bucket, so we
// unconditionally use typed v1.

const (
	TagSCCodeEntryV1 byte = 0x03

	sccodeHeaderSize = 1 + 8 // tag + BE InstallHeight
)

// ErrInvalidSCCodeEntry marks a decode error on a supposed SCCodeEntry.
var ErrInvalidSCCodeEntry = errors.New("invalid SCCodeEntry encoding")

// MarshalTyped encodes e into a freshly-allocated byte slice. Caller
// hands the slice to bbolt.Put, which copies into the page.
func (e *SCCodeEntry) MarshalTyped() []byte {
	buf := make([]byte, sccodeHeaderSize, sccodeHeaderSize+len(e.Code))
	buf[0] = TagSCCodeEntryV1
	binary.BigEndian.PutUint64(buf[1:9], int64Bits(e.InstallHeight))
	return append(buf, e.Code...)
}

// MarshalTypedAppend writes into dst, returning the grown slice. For
// hot-path callers who recycle a keyBuf-style scratch buffer.
func (e *SCCodeEntry) MarshalTypedAppend(dst []byte) []byte {
	dst = append(dst, TagSCCodeEntryV1)
	dst = binary.BigEndian.AppendUint64(dst, int64Bits(e.InstallHeight))
	dst = append(dst, e.Code...)
	return dst
}

// UnmarshalTyped populates e from b. One string allocation (the code); no
// header allocations. Returns ErrInvalidSCCodeEntry on tag mismatch or
// truncated buffer.
func (e *SCCodeEntry) UnmarshalTyped(b []byte) error {
	if len(b) < sccodeHeaderSize || b[0] != TagSCCodeEntryV1 {
		return ErrInvalidSCCodeEntry
	}
	e.InstallHeight = int64FromBits(binary.BigEndian.Uint64(b[1:9]))
	e.Code = string(b[sccodeHeaderSize:])
	return nil
}

// IsSCCodeEntryTyped reports whether b is the v1 typed encoding. Held for
// symmetry with the other typed detectors; sccode has no legacy records so
// the check is mostly defensive.
func IsSCCodeEntryTyped(b []byte) bool {
	return len(b) >= 1 && b[0] == TagSCCodeEntryV1
}

// ============================================================================
// TELAContentEntry — typed v1 encoding
// ============================================================================
//
// Tier-2 durable cache value for one (scid, path) TELA-DOC body. Body is the
// dominant payload and is written verbatim to the HTTP ResponseWriter, so on
// decode it is copied into its OWN allocation — it must never alias the
// bbolt-owned page b (freed when the View txn returns) nor the interned
// MIME/ETag buffer. MIME and ETag are length-prefixed and coalesced into one
// owned buffer (unsafe.String views), mirroring the other typed decoders.
//
// Wire layout:
//
//   byte 0          : tag 0x07
//   bytes 1..8      : Height (BE int64)
//   uvarint + bytes : MIME
//   uvarint + bytes : ETag
//   trailing raw    : Body
//
// Backward compat: legacy records are msgpack fixmaps (first byte 0x80-0x8f).
// Tag 0x07 is a msgpack positive-fixint, never a map header, so GetTELAContent
// dispatches unambiguously on byte[0].

const (
	TagTELAContentV1 byte = 0x07

	telaContentHeaderSize = 1 + 8 // tag + BE Height
)

// ErrInvalidTELAContentEntry marks a decode error on a supposed TELAContentEntry.
var ErrInvalidTELAContentEntry = errors.New("invalid TELAContentEntry encoding")

// MarshalTyped encodes e into a freshly-allocated, exactly-sized byte slice.
// One allocation: the sizing pass guarantees the append chain never regrows.
func (e *TELAContentEntry) MarshalTyped() []byte {
	size := telaContentHeaderSize +
		uvarintLen(uint64(len(e.MIME))) + len(e.MIME) +
		uvarintLen(uint64(len(e.ETag))) + len(e.ETag) +
		len(e.Body)
	buf := make([]byte, telaContentHeaderSize, size)
	buf[0] = TagTELAContentV1
	binary.BigEndian.PutUint64(buf[1:9], int64Bits(e.Height))
	buf = binary.AppendUvarint(buf, uint64(len(e.MIME)))
	buf = append(buf, e.MIME...)
	buf = binary.AppendUvarint(buf, uint64(len(e.ETag)))
	buf = append(buf, e.ETag...)
	buf = append(buf, e.Body...)
	return buf
}

// UnmarshalTyped populates e from b. At most two allocations: Body is copied
// into its own slice (it outlives b and is handed to the ResponseWriter), and
// MIME+ETag are coalesced into one owned buffer viewed via unsafe.String. When
// both MIME and ETag are empty the coalesce buffer is skipped.
func (e *TELAContentEntry) UnmarshalTyped(b []byte) error {
	if len(b) < telaContentHeaderSize || b[0] != TagTELAContentV1 {
		return ErrInvalidTELAContentEntry
	}
	e.Height = int64FromBits(binary.BigEndian.Uint64(b[1:9]))
	pos := telaContentHeaderSize
	mStart, mEnd, next, err := locLenString(b, pos)
	if err != nil {
		return ErrInvalidTELAContentEntry
	}
	pos = next
	tStart, tEnd, next, err := locLenString(b, pos)
	if err != nil {
		return ErrInvalidTELAContentEntry
	}
	pos = next
	// MIME + ETag coalesced into one owned buffer (unsafe.String views).
	total := (mEnd - mStart) + (tEnd - tStart)
	var sbuf []byte
	if total > 0 {
		sbuf = make([]byte, total)
	}
	off := 0
	e.MIME = ownedCut(sbuf, &off, b, mStart, mEnd)
	e.ETag = ownedCut(sbuf, &off, b, tStart, tEnd)
	// Body MUST be its OWN copy — never alias b nor the interned MIME/ETag buffer.
	if n := len(b) - pos; n > 0 {
		body := make([]byte, n)
		copy(body, b[pos:])
		e.Body = body
	} else {
		e.Body = nil
	}
	return nil
}

// IsTELAContentTyped reports whether b is the v1 typed encoding (vs a legacy
// msgpack fixmap). Used by GetTELAContent to dispatch reads.
func IsTELAContentTyped(b []byte) bool {
	return len(b) >= 1 && b[0] == TagTELAContentV1
}

func int64Bits(n int64) uint64 {
	return uint64(n) // #nosec G115 -- fixed-width storage preserves the int64 bit pattern.
}

func int64FromBits(n uint64) int64 {
	return int64(n) // #nosec G115 -- inverse of int64Bits for typed storage round-trips.
}

func itoaBase10(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
