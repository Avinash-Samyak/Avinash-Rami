app.service('CRUD_OperService', function ($http) {

    //Create new record
    this.post = function (Student) {      
        var request = $http({
            method: "post",
            url: "/api/StudentsAPI",
            data: Student
        });
        return request;
    }

    //Get Single Records
    this.get = function (StudentID) {
        return $http.get("/api/StudentsAPI/" + StudentID);
    }

    //Get All Student
    this.getAllStudent = function () {
        return $http.get("/api/StudentsAPI");
    }

    //Update the Record
    this.put = function (StudentID, Student) {
        var request = $http({
            method: "put",
            url: "/api/StudentsAPI/" + StudentID,
            data: Student
        });
        return request;
    }

    //Delete the Record
    this.delete = function (StudentID) {
        var request = $http({
            method: "delete",
            url: "/api/StudentsAPI/" + StudentID
        });
        return request;
    }
});