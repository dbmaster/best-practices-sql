import io.dbmaster.testng.BaseToolTestNGCase;

import org.testng.annotations.Test

import com.branegy.tools.api.ExportType;


public class BestPractiesSimpleIT extends BaseToolTestNGCase {

    @Test
    public void test() {
        def config ="""
                    <config>
                        <checkSet scope=\"all\">
                            <check name=\"FragmentedIndexes\">
                                <property name=\"min_fragmentation\" value=\"40\" />
                                <property name=\"min_page_count\"    value=\"501\" />
                            </check>
                            <check name=\"UserObjectSystemDB\" />
                        </checkSet>
                    </config>"""

        def parameters = [ "p_config"     :  config ]
        String found_tables = tools.toolExecutor("db-best-practices", parameters).execute()
        System.out.println("done! "+found_tables)
    }
}
