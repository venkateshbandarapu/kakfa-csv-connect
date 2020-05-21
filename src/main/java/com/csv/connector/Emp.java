package com.csv.connector;

public class Emp {
    public Emp(int emp_id,String emp_name,double salary) {
        this.emp_id=emp_id;
        this.emp_name=emp_name;
        this.emp_sal=salary;
    }

    public double getEmp_sal() {
        return emp_sal;
    }

    public int getEmp_id() {
        return emp_id;
    }

    public int emp_id;

    public String getEmp_name() {
        return emp_name;
    }

    public String emp_name;
    public double emp_sal;
}
