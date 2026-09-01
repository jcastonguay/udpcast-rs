const GF_BITS: usize = 8;
const GF_SIZE: usize = (1 << GF_BITS) - 1;

static ALL_PP: &[&str] = &[
    "", "", "111", "1101", "11001", "101001", "1100001", "10010001",
    "101110001", "1000100001", "10010000001", "101000000001",
    "1100101000001", "11011000000001", "110000100010001",
    "1100000000000001", "11010000000010001",
];

static mut GF_EXP: [u8; 512] = [0; 512];
static mut GF_LOG: [i32; 257] = [0; 257];
static mut INVERSE: [u8; 257] = [0; 257];
static mut GF_MUL_TABLE: [u8; 65536] = [0; 65536];

#[allow(dead_code)]
fn modnn(mut x: usize) -> u8 {
    while x >= GF_SIZE {
        x -= GF_SIZE;
        x = (x >> GF_BITS) + (x & GF_SIZE);
    }
    x as u8
}

unsafe fn generate_gf() {
    let pp = ALL_PP[GF_BITS];
    let mut mask: u8 = 1;
    unsafe { GF_EXP[GF_BITS] = 0 };

    for i in 0..GF_BITS {
        unsafe {
            GF_EXP[i] = mask;
            GF_LOG[mask as usize] = i as i32;
            if pp.as_bytes().get(i) == Some(&b'1') {
                GF_EXP[GF_BITS] ^= mask;
            }
        }
        mask <<= 1;
    }
    unsafe {
        GF_LOG[GF_EXP[GF_BITS] as usize] = GF_BITS as i32;
    }
    let mask = 1u8 << (GF_BITS - 1);
    for i in (GF_BITS + 1)..GF_SIZE {
        unsafe {
            if GF_EXP[i - 1] >= mask {
                GF_EXP[i] = GF_EXP[GF_BITS] ^ ((GF_EXP[i - 1] ^ mask) << 1);
            } else {
                GF_EXP[i] = GF_EXP[i - 1] << 1;
            }
            GF_LOG[GF_EXP[i] as usize] = i as i32;
        }
    }
    unsafe {
        GF_LOG[0] = GF_SIZE as i32;
        for i in 0..GF_SIZE {
            GF_EXP[i + GF_SIZE] = GF_EXP[i];
        }
        INVERSE[0] = 0;
        INVERSE[1] = 1;
        for i in 2..=GF_SIZE {
            INVERSE[i] = GF_EXP[GF_SIZE - GF_LOG[i] as usize];
        }
    }
}

unsafe fn init_mul_table() {
    for i in 0..=GF_SIZE {
        for j in 0..=GF_SIZE {
            unsafe {
                GF_MUL_TABLE[(i << 8) + j] =
                    GF_EXP[(GF_LOG[i] + GF_LOG[j]) as usize % GF_SIZE];
            }
        }
    }
    for j in 0..=GF_SIZE {
        unsafe {
            GF_MUL_TABLE[j] = 0;
            GF_MUL_TABLE[j << 8] = 0;
        }
    }
}

static mut FEC_INITIALIZED: bool = false;

pub fn fec_init() {
    unsafe {
        if FEC_INITIALIZED {
            return;
        }
        generate_gf();
        init_mul_table();
        FEC_INITIALIZED = true;
    }
}

#[inline]
unsafe fn gf_mul(x: u8, y: u8) -> u8 {
    unsafe { GF_MUL_TABLE[((x as usize) << 8) + y as usize] }
}

unsafe fn addmul(dst: &mut [u8], src: &[u8], c: u8, sz: usize) {
    if c == 0 {
        return;
    }
    for i in 0..sz {
        dst[i] ^= gf_mul(c, src[i]);
    }
}

unsafe fn mul(dst: &mut [u8], src: &[u8], c: u8, sz: usize) {
    if c == 0 {
        dst[..sz].fill(0);
        return;
    }
    for i in 0..sz {
        dst[i] = gf_mul(c, src[i]);
    }
}

unsafe fn invert_mat(src: &mut [u8], k: usize) -> i32 {
    let mut indxc = vec![0i32; k];
    let mut indxr = vec![0i32; k];
    let mut ipiv = vec![0i32; k];
    let mut id_row = vec![0u8; k];

    for col in 0..k {
        let mut irow: i32 = -1;
        let mut icol: i32 = -1;

        if ipiv[col] != 1 && src[col * k + col] != 0 {
            irow = col as i32;
            icol = col as i32;
        } else {
            'outer: for row in 0..k {
                if ipiv[row] != 1 {
                    for ix in 0..k {
                        if ipiv[ix] == 0 {
                            if src[row * k + ix] != 0 {
                                irow = row as i32;
                                icol = ix as i32;
                                break 'outer;
                            }
                        } else if ipiv[ix] > 1 {
                            return 1;
                        }
                    }
                }
            }
        }

        if icol == -1 {
            return 1;
        }

        let icol = icol as usize;
        let irow = irow as usize;
        ipiv[icol] += 1;

        if irow != icol {
            for ix in 0..k {
                src.swap(irow * k + ix, icol * k + ix);
            }
        }
        indxr[col] = irow as i32;
        indxc[col] = icol as i32;

        let pivot_icol = src[icol * k + icol];
        if pivot_icol == 0 {
            return 1;
        }
        if pivot_icol != 1 {
            let c = unsafe { INVERSE[pivot_icol as usize] };
            src[icol * k + icol] = 1;
            for ix in 0..k {
                src[icol * k + ix] = gf_mul(c, src[icol * k + ix]);
            }
        }

        id_row[icol] = 1;
        if src[icol * k..icol * k + k] != id_row[..] {
            for ix in 0..k {
                if ix != icol {
                    let c = src[ix * k + icol];
                    src[ix * k + icol] = 0;
                    // Use split_at_mut to get both rows without borrow conflict
                    if ix < icol {
                        let (before, after) = src.split_at_mut(icol * k);
                        let dst = &mut before[ix * k..ix * k + k];
                        let srcrow = &after[0..k];
                        addmul(dst, srcrow, c, k);
                    } else {
                        let (before, after) = src.split_at_mut(ix * k);
                        let srcrow = &before[icol * k..icol * k + k];
                        let dst = &mut after[0..k];
                        addmul(dst, srcrow, c, k);
                    }
                }
            }
        }
        id_row[icol] = 0;
    }

    for col in (0..k).rev() {
        let r = indxr[col] as usize;
        let c = indxc[col] as usize;
        if r != c {
            for row in 0..k {
                src.swap(row * k + r, row * k + c);
            }
        }
    }
    0
}

pub fn fec_encode(
    block_size: usize,
    data_blocks: &[&[u8]],
    fec_blocks: &mut [&mut [u8]],
) {
    unsafe {
        assert!(FEC_INITIALIZED);
        assert!(data_blocks.len() <= 128);
        assert!(fec_blocks.len() <= 128);
        if data_blocks.is_empty() {
            return;
        }
        for row in 0..fec_blocks.len() {
            mul(&mut fec_blocks[row], data_blocks[0], INVERSE[128 ^ row], block_size);
        }
        for (block_no, data_block) in data_blocks.iter().enumerate().skip(1) {
            let col = 128 + block_no;
            for row in 0..fec_blocks.len() {
                addmul(fec_blocks[row], data_block, INVERSE[row ^ col], block_size);
            }
        }
    }
}

pub fn fec_decode(
    block_size: usize,
    data_blocks: &mut [&mut [u8]],
    fec_blocks: &[&[u8]],
    fec_block_nos: &[u32],
    erased_blocks: &[u32],
) -> bool {
    let nr_fec_blocks = fec_blocks.len();
    let nr_data_blocks = data_blocks.len();

    let mut reduced_fec: Vec<Vec<u8>> = fec_blocks.iter().map(|b| b.to_vec()).collect();

    let mut erased_idx = 0;
    for col in 0..nr_data_blocks {
        if erased_idx < nr_fec_blocks && erased_blocks[erased_idx] as usize == col {
            erased_idx += 1;
        } else {
            let src = &data_blocks[col];
            for j in 0..nr_fec_blocks {
                let blno = fec_block_nos[j];
                unsafe {
                    addmul(&mut reduced_fec[j], src, INVERSE[(blno as usize) ^ col ^ 128], block_size);
                }
            }
        }
    }

    let mut matrix = vec![0u8; nr_fec_blocks * nr_fec_blocks];
    for (row, fec_no) in fec_block_nos.iter().enumerate() {
        let irow = 128 + *fec_no as usize;
        for (col, &erased) in erased_blocks.iter().enumerate() {
            let icol = erased as usize;
            unsafe {
                matrix[row * nr_fec_blocks + col] = INVERSE[irow ^ icol];
            }
        }
    }

    unsafe {
        let r = invert_mat(&mut matrix, nr_fec_blocks);
        if r != 0 {
            return false;
        }
    }

    for (row, &erased) in erased_blocks.iter().enumerate() {
        let target = &mut data_blocks[erased as usize];
        unsafe {
            mul(target, &reduced_fec[0], matrix[row * nr_fec_blocks], block_size);
            for col in 1..nr_fec_blocks {
                addmul(target, &reduced_fec[col], matrix[row * nr_fec_blocks + col], block_size);
            }
        }
    }
    true
}

/// The exact text the C `udp-sender -L` / `udp-receiver -L` print
/// (`fec.c::fec_license()` in the 2012 reference: GPL header for udpcast
/// plus the BSD license of the Reed-Solomon code).
///
/// Note: the const below is deliberately *not* re-indented: the GPL part
/// keeps its three-space indent, the BSD part is unindented and the blank
/// lines are truly empty, so the output stays byte-identical to what the
/// C `udp-sender -L` / `udp-receiver -L` print.

const LICENSE_TEXT: &str = r#"   udpcast and its FEC code are free software

   you can redistribute udpcast core functionality and/or
   it them under the terms of the GNU General Public License as
   published by the Free Software Foundation; either version 2 of
   the License, or (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; see the file COPYING.
   If not, write to the Free Software Foundation, Inc.,
   59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.

   Alain Knaff
   <alain@knaff.lu>
   http://udpcast.linux.lu/

the FEC code is covered by the following license:
fec.c -- forward error correction based on Vandermonde matrices
980624
(C) 1997-98 Luigi Rizzo (luigi@iet.unipi.it)
(C) 2001 Alain Knaff (alain@knaff.lu)

Portions derived from code by Phil Karn (karn@ka9q.ampr.org),
Robert Morelos-Zaragoza (robert@spectra.eng.hawaii.edu) and Hari
Thirumoorthy (harit@spectra.eng.hawaii.edu), Aug 1995

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:

1. Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above
   copyright notice, this list of conditions and the following
   disclaimer in the documentation and/or other materials
   provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE AUTHORS ``AS IS'' AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHORS
BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
"#;

/// The license text, byte-identical to the C `fec_license()` output.
pub fn fec_license_text() -> &'static str {
    LICENSE_TEXT
}

/// C `udp-sender -L` / `udp-receiver -L`: print the license and stop,
/// before the transfer is set up (`udp-sender.c` case 'L' calls this
/// straight out of the option-parsing loop and `exit(0)`s).
pub fn fec_license() {
    eprint!("{}", fec_license_text());
    std::process::exit(0);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fec_round_trip() {
        fec_init();
        let block_size = 256usize;
        let k = 10usize; // data blocks
        let r = 4usize; // redundancy blocks

        // Deterministic pseudo-random data.
        let mut data: Vec<Vec<u8>> = Vec::new();
        let mut seed: u32 = 0x1234567;
        for _ in 0..k {
            let mut blk = vec![0u8; block_size];
            for b in blk.iter_mut() {
                seed = seed.wrapping_mul(1103515245).wrapping_add(12345);
                *b = ((seed >> 16) & 0xff) as u8;
            }
            data.push(blk);
        }
        let original = data.clone();

        let mut fec: Vec<Vec<u8>> = vec![vec![0u8; block_size]; r];
        {
            let data_ptrs: Vec<&[u8]> = data.iter().map(|b| b.as_slice()).collect();
            let mut fec_ptrs: Vec<&mut [u8]> = fec.iter_mut().map(|b| b.as_mut_slice()).collect();
            fec_encode(block_size, &data_ptrs, &mut fec_ptrs);
        }

        // Erase blocks 1, 4, 7 (three erasures, <= r).
        let erased: Vec<u32> = vec![1, 4, 7];
        for &e in &erased {
            data[e as usize] = vec![0u8; block_size];
        }

        // Use the first `erased.len()` FEC blocks; their redundancy indices are 0..n.
        let n = erased.len();
        let fec_ptrs: Vec<&[u8]> = fec[..n].iter().map(|b| b.as_slice()).collect();
        let fec_nos: Vec<u32> = (0..n as u32).collect();

        let mut data_ptrs: Vec<&mut [u8]> = data.iter_mut().map(|b| b.as_mut_slice()).collect();
        let ok = fec_decode(block_size, &mut data_ptrs, &fec_ptrs, &fec_nos, &erased);
        assert!(ok, "fec_decode should succeed");

        for &e in &erased {
            assert_eq!(
                data[e as usize], original[e as usize],
                "erased block {} not recovered",
                e
            );
        }
    }

    /// Multi-stripe encode/decode using the exact block numbering the
    /// sender/receiver use on the wire: for stripes S and redundancy R the
    /// parity block of (stripe s, parity r) is numbered `s + r*S`
    /// (`senddata::fec_encode_slice` / `send_fec_blocks`), and the receiver
    /// inverts it with `bno % S` / `bno / S`
    /// (`receivedata::try_fec_recover`).
    #[test]
    fn fec_multi_stripe_wire_layout() {
        fec_init();
        let block_size = 256usize;
        let nr_data = 12usize; // per slice
        let stripes = 2usize;
        let redundancy = 3usize;

        // Deterministic pseudo-random data, one "slice" of 12 blocks.
        let mut data: Vec<Vec<u8>> = Vec::new();
        let mut seed: u32 = 0xABC0D;
        for _ in 0..nr_data {
            let mut blk = vec![0u8; block_size];
            for b in blk.iter_mut() {
                seed = seed.wrapping_mul(1103515245).wrapping_add(12345);
                *b = ((seed >> 16) & 0xff) as u8;
            }
            data.push(blk);
        }
        let original = data.clone();

        // Encode exactly like the sender: per stripe, the data blocks at
        // global positions stripe, stripe+stripes, ... ; parity r written to
        // global parity index stripe + r*stripes.
        let mut fec = vec![vec![0u8; block_size]; stripes * redundancy];
        for stripe in 0..stripes {
            let positions: Vec<usize> = (stripe..nr_data).step_by(stripes).collect();
            let data_ptrs: Vec<&[u8]> =
                positions.iter().map(|&p| data[p].as_slice()).collect();
            // Disjoint borrows in ascending index order: for this stripe the
            // parity indices stripe + r*stripes come out in r = 0,1,....
            let mut fec_ptrs: Vec<&mut [u8]> = fec
                .iter_mut()
                .enumerate()
                .filter(|(i, _)| *i % stripes == stripe)
                .map(|(_, b)| b.as_mut_slice())
                .collect();
            fec_encode(block_size, &data_ptrs, &mut fec_ptrs);
        }

        // Erase one block from each stripe: global block 4 (stripe 0, third
        // per-stripe position) and global block 3 (stripe 1, second).
        let erased_by_stripe: Vec<Vec<usize>> = vec![vec![2], vec![1]]; // per-stripe positions
        let erased_global: Vec<usize> = erased_by_stripe
            .iter()
            .enumerate()
            .flat_map(|(stripe, es)| es.iter().map(move |&j| stripe + j * stripes))
            .collect();
        for g in &erased_global {
            data[*g] = vec![0u8; block_size];
        }

        // Decode per stripe, deriving stripe/parity from the wire block
        // numbers the way the receiver does.
        for (stripe, es) in erased_by_stripe.iter().enumerate() {
            let nr_stripe = nr_data / stripes; // even interleave in this test
            let mut data_blocks: Vec<Vec<u8>> = (0..nr_stripe)
                .map(|_| vec![0u8; block_size])
                .collect();
            for (j, p) in (stripe..nr_data).step_by(stripes).enumerate() {
                if !es.contains(&j) {
                    data_blocks[j].copy_from_slice(&data[p]);
                }
            }
            let erased: Vec<u32> = es.iter().map(|&j| j as u32).collect();
            let mut fec_sel: Vec<&[u8]> = Vec::new();
            let mut fec_nos: Vec<u32> = Vec::new();
            for (bno, blk) in fec.iter().enumerate() {
                if bno % stripes == stripe {
                    fec_sel.push(blk.as_slice());
                    fec_nos.push((bno / stripes) as u32);
                }
            }
            fec_sel.truncate(es.len());
            fec_nos.truncate(es.len());
            let mut data_ptrs: Vec<&mut [u8]> =
                data_blocks.iter_mut().map(|b| b.as_mut_slice()).collect();
            let ok = fec_decode(block_size, &mut data_ptrs, &fec_sel, &fec_nos, &erased);
            assert!(ok, "stripe {} did not decode", stripe);
            for &j in es {
                let p = stripe + j * stripes;
                assert_eq!(
                    data_blocks[j],
                    original[p],
                    "erased global block {} not recovered",
                    p
                );
            }
        }
    }

    /// `-L` must print exactly what the C binary prints: 54 lines, GPL
    /// header for udpcast followed by the BSD license of the Reed-Solomon
    /// code (Rizzo/Karn/Morelos-Zarozo).
    #[test]
    fn license_text_matches_c() {
        let t = fec_license_text();
        let lines: Vec<&str> = t.lines().collect();

        assert!(t.ends_with('\n'), "C output ends with a newline");
        assert_eq!(lines.len(), 54, "C prints 54 lines");
        assert_eq!(lines[0], "   udpcast and its FEC code are free software");
        assert_eq!(
            lines[1],
            "",
            "second line is blank (C uses its own indentation)"
        );
        assert_eq!(lines[15], "   59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.");
        assert_eq!(lines[17], "   Alain Knaff");
        assert_eq!(lines[21], "the FEC code is covered by the following license:");
        assert_eq!(
            lines[22],
            "fec.c -- forward error correction based on Vandermonde matrices"
        );
        assert_eq!(lines[23], "980624");
        assert_eq!(lines[24], "(C) 1997-98 Luigi Rizzo (luigi@iet.unipi.it)");
        assert_eq!(lines[25], "(C) 2001 Alain Knaff (alain@knaff.lu)");
        assert_eq!(
            lines[27],
            "Portions derived from code by Phil Karn (karn@ka9q.ampr.org),"
        );
        assert_eq!(
            lines[31],
            "Redistribution and use in source and binary forms, with or without"
        );
        assert_eq!(
            lines[32],
            "modification, are permitted provided that the following conditions"
        );
        assert_eq!(lines[42], "THIS SOFTWARE IS PROVIDED BY THE AUTHORS ``AS IS'' AND");
        assert_eq!(lines[53], "OF SUCH DAMAGE.");

        for needle in [
            "GNU General Public License",
            "Rizzo",
            "Karn",
            "Morelos-Zaragoza",
            "Thirumoorthy",
        ] {
            assert!(t.contains(needle), "missing {} in license text", needle);
        }
    }
}
