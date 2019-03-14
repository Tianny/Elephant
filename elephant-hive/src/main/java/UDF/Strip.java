package UDF;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * A UDF must be a subclass of org.apache.hadoop.hive.ql.exec.UDF.
 * A UDF must implement at least one evaluate() method
 */

public class Strip extends UDF {
    private Text result = new Text();

    /**
     * leading and trailing whitespace from the input
     * @param str
     * @return
     */
    public Text evalute(Text str) {
        if (str == null) {
            return null;
        }
        result.set(StringUtils.strip(str.toString()));
        return result;
    }

    /**
     * strip any of a set of supplied characters from the ends of the string
     * @param str
     * @param stripChars
     * @return
     */
    public Text evaluate(Text str, String stripChars) {
        result.set(StringUtils.strip(str.toString(), stripChars));
        return result;
    }
}
