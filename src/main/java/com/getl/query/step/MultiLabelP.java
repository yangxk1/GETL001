package com.getl.query.step;

import com.getl.model.LPG.LPGElement;
import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.io.Serializable;
import java.util.function.BiPredicate;

/**
 * https://tinkerpop.apache.org/docs/current/reference/#_multi_label
 */
public final class MultiLabelP extends P<String> {

    private MultiLabelP(final String label) {
        super(LabelBiPredicate.instance(), label);
    }

    public static P<String> of(final String label) {
        return new MultiLabelP(label);
    }

    public static final class LabelBiPredicate implements BiPredicate<String, String>, Serializable {

        private static final LabelBiPredicate INSTANCE = new LabelBiPredicate();

        private LabelBiPredicate() {
        }

        /**
         * 判断是否存在label
         *
         * @param testLabels element 的 label
         * @param thisLabel  参数中的label
         * @return
         */
        @Override
        public boolean test(final String testLabels, final String thisLabel) {
            return testLabels.equals(thisLabel) || testLabels.contains(LPGElement.LABEL_SPLITTER + thisLabel) || testLabels.contains(thisLabel + LPGElement.LABEL_SPLITTER);
        }

        public static LabelBiPredicate instance() {
            return INSTANCE;
        }
    }

}
