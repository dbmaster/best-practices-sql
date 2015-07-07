import io.dbmaster.testng.BaseToolTestNGCase;

import org.testng.annotations.Test

import com.branegy.tools.api.ExportType;


public class BestPractiesSimpleIT extends BaseToolTestNGCase {

    @Test
    public void test() {
        def parameters = [ "p_servers"     :  getTestProperty("p_servers") ]
        String found_tables = tools.toolExecutor("db-best-practices", parameters).execute()
        System.out.println("done! "+found_tables)
    }
}
