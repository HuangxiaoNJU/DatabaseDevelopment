package main;

import dao.Create;
import dao.InitData;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
        Create create = Create.getInstance();
        InitData initData = InitData.getInstance();

        long start = System.currentTimeMillis();
        create.createUserTable();
        create.createBikeTable();
        create.createRecordTable();
        create.createRepairTable();
        System.out.println("建表时间：\t" + (System.currentTimeMillis() - start) / 1000.0 + "s");

        start = System.currentTimeMillis();
        initData.initBikeData();
        System.out.println("bike数据初始化时间：\t" + (System.currentTimeMillis() - start) / 1000.0 + "s");

        start = System.currentTimeMillis();
        initData.initUserData();
        System.out.println("user数据初始化时间：\t" + (System.currentTimeMillis() - start) / 1000.0 + "s");

        start = System.currentTimeMillis();
        initData.initRecordData();
        System.out.println("record数据初始化时间：\t" + (System.currentTimeMillis() - start) / 1000.0 + "s");

        start = System.currentTimeMillis();
        initData.addUserHome();
        System.out.println("添加住址时间：\t" + (System.currentTimeMillis() - start) / 1000.0 + "s");

        start = System.currentTimeMillis();
        initData.addBikeRepair();
        System.out.println("单车维护数据初始化时间：\t" + (System.currentTimeMillis() - start) / 1000.0 + "s");
    }

}
