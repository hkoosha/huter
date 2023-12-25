package io.koosha.huter;

import io.koosha.huter.util.HuterUtil;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

public final class TableLocationFixerHook implements HiveSemanticAnalyzerHook {

    private static final Logger LOG = LoggerFactory.getLogger(TableLocationFixerHook.class);

    private static Path nextTableLocation = null; // single threaded

    public static void prepareAndRememberNextTable(final Path nextTable) throws IOException {
        Objects.requireNonNull(nextTable);
        LOG.trace("prepareAndRememberNextTable(nextTable={})", nextTable);
        nextTableLocation = nextTable;
        HuterUtil.recreateDir(nextTable);
    }

    @Override
    public ASTNode preAnalyze(final HiveSemanticAnalyzerHookContext paramHiveSemanticAnalyzerHookContext,
                              final ASTNode paramASTNode) {
        // Why is the || needed? nasty bug? in debugger says == is true, but
        // the jvm evaluates it as false anyway!!!
        // One of the most annoying sh*t I ever witnessed.
        if (paramASTNode.getType() == HiveParser.TOK_CREATETABLE
            || "TOK_CREATETABLE".equals(paramASTNode.getText())) {
            ASTNode location = null;
            for (final Node node : paramASTNode.getChildren()) {
                final ASTNode astNode = (ASTNode) node;
                if (astNode.getType() == HiveParser.TOK_TABLELOCATION
                    || "TOK_TABLELOCATION".equals(astNode.getText())) {
                    location = astNode;
                    break;
                }
            }

            if (location != null) {
                final ASTNode locPath = (ASTNode) location.getChild(0);
                locPath.token.setText('"' + "file://" + nextTableLocation.toString() + '"');
            }
        }

        return paramASTNode;
    }

    @Override
    public void postAnalyze(HiveSemanticAnalyzerHookContext paramHiveSemanticAnalyzerHookContext,
                            List<Task<? extends Serializable>> paramList) {
    }

}
