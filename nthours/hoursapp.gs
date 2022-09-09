// project: hoursApp
// declarations
var ss = SpreadsheetApp.getActiveSpreadsheet();
var week_sht = ss.getSheetByName("this_week");
var month_sht = ss.getSheetByName("this_month");
var weekCol = 7;
var week_tbl_refs = ['week_roles', 'week_executive',
 'week_social_entrepreneur', 'week_basic', 'week_sleep_max'];

// menu
function onOpen() {
  var ui = SpreadsheetApp.getUi();
  ui.createMenu('hoursApp')
      .addItem('View current week','view_current_week')
      .addItem('View last week','view_last_week')
      .addItem('Update month view','update_month_view')
      .addToUi();
} 

function view_week(weekNum){
  var pvt_tbls = week_sht.getPivotTables()
  for (var i=0;i<pvt_tbls.length;i++){
    var filters = pvt_tbls[i].getFilters()
    for (var k=0;k<filters.length;k++){
      var filter = filters[k]
      var colNum = filter.getSourceDataColumn()
      if (colNum==weekCol){
        filter.remove()
        const criteria = SpreadsheetApp.newFilterCriteria()
            .setVisibleValues([weekNum])
            .build();
      
        pvt_tbls[i].addFilter(colNum, criteria);      
      }
    }
  }
}

function view_current_week() {
  // sets the pivot table views to the current week
  // by adjusting the filter criteria
  var this_week = ss.getRangeByName("this_week").getValue();
  view_week(this_week);
  // do something specific to a particular table by name
  //  var tbl_rng = pvt_tbls[i].getAnchorCell().getA1Notation()
  //  for (var j=0;j<week_tbl_refs.length;j++){
  //   var tbl_name = week_tbl_refs[j]
  //   if (tbl_rng==ss.getRangeByName(tbl_name).getA1Notation()){
  //      // if table_name = X
  //      //  do something specific for table X
  //    }
  //  }
  // }

  // Browser.msgBox(this_week)
}

function view_last_week() {
  // sets the pivot table views to last week
  // by adjusting the filter criteria
  var this_week = ss.getRangeByName("this_week").getValue();
  view_week(this_week-1);
}

function update_month_view() {
  // sets the pivot table month views based on the preconfigure weeks selected
  var weeks = ss.getRangeByName("month_weeks").getValues();
  var pvt_tbls = month_sht.getPivotTables()
  for (var i=0;i<pvt_tbls.length;i++){
    var filters = pvt_tbls[i].getFilters()
    for (var k=0;k<filters.length;k++){
      var filter = filters[k]
      var colNum = filter.getSourceDataColumn()
      if (colNum==weekCol){
        filter.remove()
        const criteria = SpreadsheetApp.newFilterCriteria()
            .setVisibleValues(weeks[0])
            .build();
      
        pvt_tbls[i].addFilter(colNum, criteria);      
      }
    }
  }
}