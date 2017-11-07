package main;

import dao.Init;
import dao.Solve;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
        Init init = Init.getInstance();
        Solve solve = Solve.getInstance();

        long start = System.currentTimeMillis();
        init.createTable();
        System.out.println("建表时间：\t" + (System.currentTimeMillis() - start) / 1000.0 + "s");
        start = System.currentTimeMillis();
        init.initData();
        System.out.println("插入数据时间：" + (System.currentTimeMillis() - start) / 1000.0 + "s");

        solve.search();
        solve.modifyCost();

        start = System.currentTimeMillis();
        solve.exchangeDormitory();
        System.out.println("互换宿舍楼执行时间：\t" + (System.currentTimeMillis() - start) / 1000.0 + "s");

    }
}
