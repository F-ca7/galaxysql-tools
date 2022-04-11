/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package worker.common;

import com.lmax.disruptor.RingBuffer;
import model.ProducerExecutionContext;
import model.config.FileFormat;
import model.config.FileLineRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import worker.common.reader.CsvReader;
import worker.common.reader.FileBufferedBatchReader;
import worker.common.reader.XlsxReader;

import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 按csv标准按行处理csv文本文件
 * 解压场景先通过解压文件来进行兜底处理
 * TODO 再套一层producer
 */
public class ReadFileWithLineProducer extends ReadFileProducer {

    private static final Logger logger = LoggerFactory.getLogger(ReadFileWithLineProducer.class);

    public ReadFileWithLineProducer(ProducerExecutionContext context,
                                    RingBuffer<BatchLineEvent> ringBuffer,
                                    List<FileLineRecord> fileLineRecordList) {
        super(context, ringBuffer, fileLineRecordList);
    }

    @Override
    public void produce() {
        // 并行度大小为文件数量
        // todo 暂时与文件数量相同 如果文件数量太多将控制并发度
        ThreadPoolExecutor threadPool = context.getProducerExecutor();
        FileFormat fileFormat = context.getFileFormat();
        FileBufferedBatchReader readFileWorker = null;
        for (int i = 0; i < fileList.size(); i++) {
            switch (fileFormat) {
            case XLSX:
                readFileWorker = new XlsxReader(context, fileList, i, ringBuffer);
                break;
            default:
                readFileWorker = new CsvReader(context, fileList, i, ringBuffer);
            }
            threadPool.submit(readFileWorker);
        }
    }

}
