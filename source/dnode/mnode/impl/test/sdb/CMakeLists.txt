add_executable(clearTrans clearTrans.c)
target_link_libraries(
    clearTrans
    PUBLIC sdb mnode
)

#add_test(
#    NAME clearTrans
#    COMMAND clearTrans
#)
