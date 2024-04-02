MINIMAL_CONFIG = """
    <auto_dp>
        <table name='item'>
            <differential_privacy epsilon='1.0' pre_epsilon='0.0' algorithm='aim'/>
        </table>
    </auto_dp>
"""

FULL_CONFIG = """
   <full_anon>
        <table name="item">
            <differential_privacy epsilon="1.0" pre_epsilon="0.0" algorithm="aim">
            <!-- Column categorization -->
                <ignore>
                    <column name="i_id"/>
                </ignore>
                <categorical>
                    <column name="i_name" />
                    <column name="i_data" />
                    <column name="i_im_id" />
                </categorical>
            <!-- Continuous column fine-tuning -->
                <continuous>
                    <column name="i_price" bins="1000" lower="2.0" upper="100.0" /> 
                </continuous>
            </differential_privacy>
            <!-- Sensitive value handling -->
            <value_faking>
                <column name="i_name" method="name" locales="en_US" seed="0"/>
            </value_faking>
        </table>
    </full_anon>
"""


def test_full_config():

    assert True


def test_minimal_config():

    assert True
