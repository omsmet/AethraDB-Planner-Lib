import calcite.rules.ArrowTableScanFilterProjectRule;
import calcite.rules.ArrowTableScanProjectionRule;
import calcite.rules.ArrowTableScanRule;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.word.Pointer;
import util.arrow.AethraQueryEncoder;
import util.arrow.ArrowSchemaBuilder;

import java.io.BufferedReader;
import java.io.FileReader;

public class PlannerEntryPoint {

    private final static HepPlanner aethraHepPlanner;

    static {
        HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
        hepProgramBuilder.addRuleInstance(AggregateReduceFunctionsRule.Config.DEFAULT.toRule());
        hepProgramBuilder.addRuleInstance(FilterJoinRule.FilterIntoJoinRule.FilterIntoJoinRuleConfig.DEFAULT.toRule());
        hepProgramBuilder.addRuleInstance(ProjectJoinTransposeRule.Config.DEFAULT.toRule());
        hepProgramBuilder.addRuleInstance(ProjectRemoveRule.Config.DEFAULT.toRule());
        final ArrowTableScanProjectionRule PROJECT_SCAN = ArrowTableScanProjectionRule.Config.DEFAULT.toRule();
        hepProgramBuilder.addRuleInstance(PROJECT_SCAN);
        final ArrowTableScanRule ARROW_SCAN = ArrowTableScanRule.Config.DEFAULT.toRule();
        hepProgramBuilder.addRuleInstance(ARROW_SCAN);
        final ArrowTableScanFilterProjectRule ARROW_SCAN_FILTER_PROJECT = ArrowTableScanFilterProjectRule.Config.DEFAULT.toRule();
        hepProgramBuilder.addRuleInstance(ARROW_SCAN_FILTER_PROJECT);

        aethraHepPlanner = new HepPlanner(hepProgramBuilder.build());
    }

    @CEntryPoint(name = "Java_AethraDB_util_AethraDatabase_plan")
    public static JNIEnv.JString plan(JNIEnv jniEnv, Pointer clazz, IsolateThread isolateThread, JNIEnv.JString rawDatabasePath, JNIEnv.JString rawQueryPath) throws Exception {
        JNIEnv.JNINativeInterface fn = jniEnv.getFunctions();
        CCharPointer cDatabasePathPointer = fn.getGetStringUTFChars().call(jniEnv, rawDatabasePath, (byte) 0);
        String databasePath = CTypeConversion.toJavaString(cDatabasePathPointer);

        CCharPointer cQueryPathPointer = fn.getGetStringUTFChars().call(jniEnv, rawQueryPath, (byte) 0);
        String queryPath = CTypeConversion.toJavaString(cQueryPathPointer);

        final String aethraPlan = internalPlan(databasePath, queryPath);

        // Return the optimised query to the caller
        try (final CTypeConversion.CCharPointerHolder holder = CTypeConversion.toCString(aethraPlan)) {
            return fn.getNewStringUTF().call(jniEnv, holder.get());
        }
    }

    @CEntryPoint(name = "Java_AethraDB_util_AethraDatabase_createIsolate", builtin=CEntryPoint.Builtin.CREATE_ISOLATE)
    public static native IsolateThread createIsolate();

    private static String internalPlan(String databasePath, String queryPath) throws Exception {
        // Read the schema from disk
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        CalciteSchema databaseSchema = ArrowSchemaBuilder.fromDirectory(databasePath, typeFactory);

        // Initialise the planner
        SqlParser.Config sqlParserConfig = SqlParser.config().withCaseSensitive(false);
        FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(sqlParserConfig)
                .defaultSchema(databaseSchema.plus())
                .build();
        PlannerImpl queryPlanner = new PlannerImpl(frameworkConfig);

        // First, parse the query
        BufferedReader queryReader = new BufferedReader(new FileReader(queryPath));
        SqlNode parsedSqlQuery = queryPlanner.parse(queryReader);

        // Next, validate the query
        SqlNode validatedSqlQuery = queryPlanner.validate(parsedSqlQuery);
        RelNode queryRoot = queryPlanner.rel(validatedSqlQuery).project();

        // Finally, plan/optimise the query
        aethraHepPlanner.setRoot(queryRoot);
        RelNode optimisedQuery = aethraHepPlanner.findBestExp();

        // Translate the query plan to the Aethra Engine Plan Format
        final String aethraPlan = AethraQueryEncoder.encode(optimisedQuery);

        // Close the planner
        queryPlanner.close();

        // Return the result
        return aethraPlan;
    }

}
