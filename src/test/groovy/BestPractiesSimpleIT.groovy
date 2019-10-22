import static org.testng.Assert.assertTrue

import org.testng.annotations.Test

import io.dbmaster.testng.BaseToolTestNGCase;


public class BestPractiesSimpleIT extends BaseToolTestNGCase {

    @Test
    public void test() {
        def config ="""
<config>
    <scope name="non-system">
       <databases include="System&lt;&gt;1" exclude="" />
       <objects    include="type=Table &amp;&amp; category=Test"  />
    </scope>
    <scope name="dev_all"  >
       <servers include="Environment=Development" />
    </scope>

    <checkSet scope="dev_all">
        <check name="FragmentedIndexes">
            <property name="min_fragmentation" value="40" />
            <property name="min_page_count"    value="501" />
        </check>
        <check name="UserObjectSystemDB">
            <property name="sysdb.valid_object"    value="msdb.dbo.sysdtslog90" />
            <property name="sysdb.valid_object"    value="master.dbo.sp_WhoIsActive" />
        </check>
    </checkSet>
    <checkSet scope="dev_all">

    </checkSet>
</config>
                    """

        def parameters = [ "p_config"     :  config ]
        String found_tables = tools.toolExecutor("best-practices-sql", parameters).execute()
        System.out.println("done! "+found_tables)
        assertTrue(found_tables.contains("Server"));
    }
}
