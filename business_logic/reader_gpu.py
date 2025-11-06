# reader_gpu.py
import os
import sys
from typing import Any, Generator
import numpy as np
import cupy as cp
from cupy import RawKernel

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.logger import AppLogger
from utils.enums import MessageType


class Reader:
    HEADER = b'\xA3\x95'
    FMT_HEADER = b'\xA3\x95\x80'
    FMT_MSG_LENGTH = 89

    TYPE_MAP = {
        'a': ('32h', 2 * 32), 'b': ('b', 1), 'B': ('B', 1), 'h': ('h', 2), 'H': ('H', 2),
        'i': ('i', 4), 'I': ('I', 4), 'f': ('f', 4), 'd': ('d', 8),
        'n': ('4s', 4), 'N': ('16s', 16), 'Z': ('64s', 64),
        'c': ('h', 2), 'C': ('H', 2), 'e': ('i', 4), 'E': ('I', 4),
        'L': ('i', 4), 'M': ('B', 1), 'q': ('q', 8), 'Q': ('Q', 8),
    }

    SCALE_100_SET = frozenset(["c", "C", "e", "E"])
    STRING_SET = frozenset(["n", "N", "Z"])
    ROUND_SET = frozenset([
        "Lat", "Lng", "TLat", "TLng", "Pitch", "IPE", "Yaw", "IPN", "IYAW",
        "DesPitch", "NavPitch", "Temp", "AltE", "VDop", "VAcc", "Roll",
        "HAGL", "SM", "VWN", "VWE", "IVT", "SAcc", "TAW", "IPD", "ErrRP",
        "SVT", "SP", "TAT", "GZ", "HDop", "NavRoll", "NavBrg", "TAsp",
        "HAcc", "DesRoll", "SH", "TBrg", "AX"
    ])

    def __init__(self) -> None:
        self.logger = AppLogger(self.__class__.__name__)
        self.fmt_messages = {}
        self.PROCESSING_CACHE = {}
        self._compile_gpu_kernel()

    def _compile_gpu_kernel(self):
        kernel_code = r'''
        extern "C" __global__
        void parse_messages(
            const unsigned char* payloads,
            const int* offsets,
            const int* lengths,
            const int* type_ids,
            const int* fmt_map,
            const char* fmt_strings,
            const int* col_map,
            const int* col_ops,
            double* results,
            int N, int max_len, int max_cols, int to_round
        ) {
            int idx = blockIdx.x * blockDim.x + threadIdx.x;
            if (idx >= N) return;

            int type_id = type_ids[idx];
            int offset = offsets[idx];
            int length = lengths[idx];
            const unsigned char* data = payloads + idx * max_len + offset;

            int fmt_offset = -1;
            for (int i = 0; i < 256; i++) {
                if (fmt_map[i*2] == type_id) { fmt_offset = fmt_map[i*2 + 1]; break; }
                if (fmt_map[i*2] == -1) break;
            }
            if (fmt_offset == -1) return;

            const char* fmt = fmt_strings + fmt_offset;
            int pos = 0;
            int col_idx = 0;
            double value = 0.0;

            for (int f = 0; fmt[f] != 0; f++) {
                char t = fmt[f];
                if (col_idx >= max_cols) break;

                int op_code = col_ops[col_idx*2];
                int round_flag = col_ops[col_idx*2 + 1];

                if (t == 'b' || t == 'B') { value = data[pos++]; }
                else if (t == 'h' || t == 'H') { value = data[pos] | (data[pos+1]<<8); if(t=='h' && value>=32768)value-=65536; pos+=2; }
                else if (t == 'i' || t == 'I') { value=data[pos]|(data[pos+1]<<8)|(data[pos+2]<<16)|(data[pos+3]<<24); if(t=='i' && value>=2147483648)value-=4294967296; pos+=4; }
                else if (t=='f'){ unsigned int ival=data[pos]|(data[pos+1]<<8)|(data[pos+2]<<16)|(data[pos+3]<<24); int sign=(ival>>31)?-1:1; int exp=(ival>>23)&0xFF; int mant=ival&0x7FFFFF; if(exp==0) value=0.0; else if(exp==255) value=(mant==0)?INFINITY:NAN; else value=sign*(1.0+mant/8388608.0)*pow(2.0,exp-127); pos+=4; }
                else if (t=='c'||t=='C'||t=='e'||t=='E'){ if(t=='c'||t=='C'){ value=data[pos]|(data[pos+1]<<8); if(t=='c'&&value>=32768)value-=65536; pos+=2; } else { value=data[pos]|(data[pos+1]<<8)|(data[pos+2]<<16)|(data[pos+3]<<24); if(t=='e'&&value>=2147483648)value-=4294967296; pos+=4; } value/=100.0; if(to_round&&round_flag) value=round(value,7); }
                else if (t=='L'){ value=data[pos]|(data[pos+1]<<8)|(data[pos+2]<<16)|(data[pos+3]<<24); if(value>=2147483648)value-=4294967296; value*=1e-7; if(to_round&&round_flag)value=round(value,7); pos+=4; }
                else { pos+=1; }

                results[idx*max_cols+col_map[col_idx]]=value;
                col_idx++;
            }
        }
        '''
        self.kernel = RawKernel(kernel_code, 'parse_messages')

    @staticmethod
    def decode_msg(data: memoryview) -> str:
        return data.tobytes().partition(b'\x00')[0].decode('ascii', errors='ignore')

    def read_fmt_massage(self, data: memoryview, start_offset: int) -> dict:
        if data[start_offset + 2] != 0x80: return {}
        fmt_type = data[start_offset + 3]
        fmt_length = data[start_offset + 4]
        fmt_name = self.decode_msg(data[start_offset + 5:start_offset + 9])
        fmt_format = self.decode_msg(data[start_offset + 9:start_offset + 25])
        fmt_cols = self.decode_msg(data[start_offset + 25:start_offset + 89])
        fmt_split_cols = fmt_cols.split(",")

        self.fmt_messages[fmt_type] = {
            "mavpackettype":"FMT",
            "Name":fmt_name,
            "Length":fmt_length,
            "Format":fmt_format,
            "Columns":fmt_cols,
            "Type":fmt_type,
            "cols":fmt_split_cols
        }
        self._compile_processing(fmt_type, fmt_format, fmt_split_cols)
        return self.fmt_messages[fmt_type]

    def _compile_processing(self, type_msg:int, types:str, cols:list[str]) -> None:
        processing = []
        for t, col in zip(types, cols):
            op = None
            if t in self.SCALE_100_SET: op=('scale100', col in self.ROUND_SET)
            elif t in self.STRING_SET and col!="Data": op=('string',0)
            elif t=="L": op=('latlon', col in self.ROUND_SET)
            processing.append(op)
        self.PROCESSING_CACHE[type_msg] = processing

    def read_messages(self, data: memoryview, to_round: bool,
                     message_type_to_read: MessageType=MessageType.ALL_MESSAGES,
                     fmt_messages=None, wanted_type:str="") -> Generator[dict[Any,Any],Any,None]:

        if fmt_messages is not None: self.fmt_messages=fmt_messages
        data_np = np.frombuffer(data,dtype=np.uint8)
        payloads, offsets, lengths, type_ids=[],[],[],[]

        i, data_len = 0, len(data_np)
        while i<data_len-3:
            if data_np[i]==0xA3 and data_np[i+1]==0x95:
                type_id=int(data_np[i+2])
                if type_id==0x80:
                    self.read_fmt_massage(data_np,i)
                    i+=self.FMT_MSG_LENGTH
                    continue
                if type_id in self.fmt_messages:
                    msg_len=int(self.fmt_messages[type_id]["Length"])
                    end_pos=i+3+msg_len
                    if end_pos<=data_len:
                        payloads.append(data_np[i+3:end_pos].copy())
                        offsets.append(0)
                        lengths.append(msg_len)
                        type_ids.append(type_id)
                    i=end_pos
                else: i+=1
            else: i+=1

        if not payloads: return
        max_len = max(lengths)
        payload_array = np.zeros((len(payloads), max_len), dtype=np.uint8)
        for j, p in enumerate(payloads): payload_array[j,:len(p)]=p

        # col_ops / col_map
        col_map, col_ops = [], []
        global_idx = 0
        for type_id in type_ids:
            processing = self.PROCESSING_CACHE.get(type_id, [])
            for op in processing:
                if op is None: col_ops.extend([0,0])
                else:
                    if op[0]=='scale100': op_code=1
                    elif op[0]=='latlon': op_code=2
                    elif op[0]=='string': op_code=3
                    else: op_code=0
                    round_flag=op[1] if len(op)>1 else 0
                    col_ops.extend([op_code,int(round_flag)])
                col_map.append(global_idx)
                global_idx+=1

        max_cols = len(col_map)
        results = np.zeros((len(payloads), max_cols), dtype=np.float64)

        # העברת GPU
        d_payloads = cp.asarray(payload_array)
        d_offsets = cp.array(offsets,dtype=cp.int32)
        d_lengths = cp.array(lengths,dtype=cp.int32)
        d_type_ids = cp.array(type_ids,dtype=cp.int32)
        d_fmt_map = cp.full((256,2),-1,dtype=cp.int32)
        d_fmt_strings = cp.array([0],dtype=cp.int8)  # פשטות
        d_col_map = cp.array(col_map,dtype=cp.int32)
        d_col_ops = cp.array(col_ops,dtype=cp.int32)
        d_results = cp.array(results)

        threads = 256
        blocks = (len(payloads)+threads-1)//threads
        self.kernel((blocks,),(threads,),(d_payloads,d_offsets,d_lengths,d_type_ids,d_fmt_map,d_fmt_strings,d_col_map,d_col_ops,d_results,len(payloads),max_len,max_cols,int(to_round)))

        results_host = d_results.get()
        # יצירת הודעות בפייתון
        for i, type_id in enumerate(type_ids):
            msg_name = self.fmt_messages[type_id]["Name"]
            if wanted_type and wanted_type!=msg_name: continue
            msg={"mavpackettype":msg_name}
            config=self.fmt_messages[type_id]
            for j,_ in enumerate(config["cols"]):
                idx=col_map[j] if j<len(col_map) else j
                msg[config["cols"][j]]=float(results_host[i,idx])
            yield msg
