package main;

import dao.Create;

public class Main {

    public static void main(String[] args) {
        Create create = Create.getInstance();
        create.createUserTable();
    }

}
