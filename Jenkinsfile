builder(
        buildTasks: [
                [
                        name: "Linters",
                        type: "lint",
                        method: "inside",
                        runAsUser: "root",
                        entrypoint: "",
                        command: [
                                "pip install --no-cache-dir -r requirements-test.txt",
                                "flake8",
                        ],
                ],
                [
                        name: 'Tests',
                        type: 'test',
                        method: 'inside',
                        runAsUser: 'root',
                        entrypoint: '',
                        environment: [
                                JSEARCH_MAIN_DB_TEST: 'postgres://app:pass@maindb/jsearch-maindb',
                                JSEARCH_RAW_DB_TEST: 'postgres://app:pass@rawdb/jsearch-rawdb',
                        ],
                        sidecars: [
                                maindb: [
                                        image: 'postgres:11.0-alpine',
                                        environment: [
                                                POSTGRES_USER: 'app',
                                                POSTGRES_PASSWORD: 'pass',
                                                POSTGRES_DB: 'jsearch-maindb',
                                        ],
                                ],
                                rawdb: [
                                        image: 'postgres:11.0-alpine',
                                        environment: [
                                                POSTGRES_USER: 'app',
                                                POSTGRES_PASSWORD: 'pass',
                                                POSTGRES_DB: 'jsearch-rawdb',
                                        ],
                                ],
                        ],
                        command: [
                                'pip install --no-cache-dir -r requirements-test.txt',
                                'pytest'
                        ],
                ]
        ],
)
