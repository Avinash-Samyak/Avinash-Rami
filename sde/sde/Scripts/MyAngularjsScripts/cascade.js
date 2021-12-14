var app = angular.module('myModule', []);
app.controller('myController', function ($scope, $http) {

    GetCountries();
    function GetCountries() {
        $http({
            method: 'Get',
            url: '/OrderM/GetCountries'
        }).success(function (data, status, headers, config) {
            $scope.countries = data;
        }).error(function (data, status, headers, config) {
            $scope.message = 'Unexpected Error1';
        });
    }

    $scope.GetStates = function () {
        var countryId = $scope.country;
        if (countryId) {
            $http({
                method: 'POST',
                url: '/OrderM/GetStates/',
                data: JSON.stringify({ countryId: countryId })
            }).success(function (data, status, headers, config) {
                $scope.states = data;
            }).error(function (data, status, headers, config) {
                $scope.message = 'Unexpected Error2';
            });
        }
        else {
            $scope.states = null;
        }
    }

    GetAll();
    function GetAll() {
        $http({
            method: 'Get',
            url: '/OrderM/GetAll'
        }).success(function (data, status, headers, config) {
            $scope.allItems = data;
        }).error(function (data, status, headers, config) {
            $scope.message = 'Unexpected Error3';
        });
    }

});