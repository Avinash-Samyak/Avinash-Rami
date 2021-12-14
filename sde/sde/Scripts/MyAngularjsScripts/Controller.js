app.controller('CRUD_OperController', function ($scope, CRUD_OperService) {
    $scope.OperType = 1;
    //1 Mean New Entry

    GetAllRecords();
    //To Get All Records
    function GetAllRecords() {
        var promiseGet = CRUD_OperService.getAllStudent();
        promiseGet.then(function (pl) { $scope.Students = pl.data },
              function (errorPl) {
                  $log.error('Some Error in Getting Records.', errorPl);
              });
    }

    //To Clear all input controls.
    function ClearModels() {
        $scope.OperType = 1;
        $scope.StudentID = "";
        $scope.Name = "";
        $scope.Email = "";
        $scope.Class = "";
        $scope.EnrollYear = "";
        $scope.City = "";
        $scope.Country = "";
    }

   //To Create new record and Edit an existing Record.
    $scope.save = function () {
        var Student = {
            Name: $scope.Name,
            Email: $scope.Email,
            Class: $scope.Class,
            EnrollYear: $scope.EnrollYear,
            City: $scope.City,
            Country: $scope.Country
        };
        if ($scope.OperType === 1) {
            var promisePost = CRUD_OperService.post(Student);
            promisePost.then(function (pl) {
                $scope.Message = "Student Added Successfuly";
                $scope.StudentID = pl.data.StudentID;
                GetAllRecords();
                ClearModels();
            }, function (err) {
                console.log("Err" + err);
            });
        } else {
            //Edit the record              
            Student.StudentID = $scope.StudentID;
            var promisePut = CRUD_OperService.put($scope.StudentID, Student);
            promisePut.then(function (pl) {
                $scope.Message = "Student Updated Successfuly";
                GetAllRecords();
                ClearModels();
            }, function (err) {
                console.log("Err" + err);
            });
        }
    };

    //To Delete Record
    $scope.delete = function (Student) {
        var promiseDelete = CRUD_OperService.delete(Student.StudentID);
        promiseDelete.then(function (pl) {
            $scope.Message = "Student Deleted Successfuly";
            GetAllRecords();
            ClearModels();
        }, function (err) {
            console.log("Err" + err);
        });
    }

    //To Get Student Detail on the Base of Student ID
    $scope.get = function (Student) {
        var promiseGetSingle = CRUD_OperService.get(Student.StudentID);

        promiseGetSingle.then(function (pl) {
            var res = pl.data;
            $scope.StudentID = res.StudentID;
            $scope.Name = res.Name;
            $scope.Email = res.Email;
            $scope.Class = res.Class;
            $scope.EnrollYear = res.EnrollYear;
            $scope.City = res.City;
            $scope.Country = res.Country;

            $scope.OperType = 0;
        },
                  function (errorPl) {
                      console.log('Some Error in Getting Details', errorPl);
                  });
    }

    //To Clear all Inputs controls value.
    $scope.clear = function () {
        $scope.OperType = 1;
        $scope.StudentID = "";
        $scope.Name = "";
        $scope.Email = "";
        $scope.Class = "";
        $scope.EnrollYear = "";
        $scope.City = "";
        $scope.Country = "";
    }

});
