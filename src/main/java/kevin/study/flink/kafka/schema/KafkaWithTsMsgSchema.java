package kevin.study.flink.kafka.schema;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;

public class KafkaWithTsMsgSchema implements DeserializationSchema<Tuple2<String, Long>>, SerializationSchema<Tuple2<String, Long>> {
    private static final long serialVersionUID = 1L;
    private transient Charset charset;

    public KafkaWithTsMsgSchema() {
        this(Charset.forName("UTF-8"));
    }

    public KafkaWithTsMsgSchema(Charset charset) {
        this.charset = Preconditions.checkNotNull(charset);
    }

    public Charset getCharset() {
        return this.charset;
    }

    public Tuple2<String, Long> deserialize(byte[] message) {
        String msg = new String(message, charset);
        System.out.println("receive message : " + msg);
        String[] dataAndTs = msg.split("#");
        if(dataAndTs.length == 2){
            return new Tuple2<String, Long>(dataAndTs[0], Long.parseLong(dataAndTs[1].trim()));
        }else{
            // 实际生产上需要抛出runtime异常
            System.out.println("Fail due to invalid msg format.. ["+msg+"]");
            return new Tuple2<String, Long>(msg, 0L);
        }
    }

    @Override
    public boolean isEndOfStream(Tuple2<String, Long> stringLongTuple2) {
        return false;
    }

    public byte[] serialize(Tuple2<String, Long> element) {
        return "MAX - ".concat(element.f0).concat("#").concat(String.valueOf(element.f1)).getBytes(this.charset);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeUTF(this.charset.name());
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        String charsetName = in.readUTF();
        this.charset = Charset.forName(charsetName);
    }

    @Override
    public TypeInformation<Tuple2<String, Long>> getProducedType() {
        return new TupleTypeInfo<Tuple2<String, Long>>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO);
    }
}